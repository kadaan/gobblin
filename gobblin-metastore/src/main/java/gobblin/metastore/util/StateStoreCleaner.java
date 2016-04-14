/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.metastore.util;

import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import gobblin.util.HadoopUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.google.common.io.Files;
import com.google.common.util.concurrent.AbstractIdleService;

import gobblin.configuration.ConfigurationKeys;
import gobblin.util.concurrent.GobblinRunnable;
import gobblin.util.ExecutorsUtils;


/**
 * A utility class for cleaning up old state store files created by {@link gobblin.metastore.FsStateStore}
 * based on a configured retention.
 *
 * @author Yinan Li
 */
@Slf4j
public class StateStoreCleaner extends AbstractIdleService implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(StateStoreCleaner.class);

  private static final String STATE_STORE_CLEANER_RETENTION_KEY = "state.store.retention";
  private static final String DEFAULT_STATE_STORE_CLEANER_RETENTION = "7";
  private static final String STATE_STORE_CLEANER_RETENTION_TIMEUNIT_KEY = "state.store.retention.timeunit";
  private static final String DEFAULT_STATE_STORE_CLEANER_RETENTION_TIMEUNIT = TimeUnit.DAYS.toString();
  private static final String STATE_STORE_CLEANER_EXECUTOR_THREADS_KEY = "state.store.cleaner.executor.threads";
  private static final String DEFAULT_STATE_STORE_CLEANER_EXECUTOR_THREADS = "50";
  private static final String STATE_STORE_CLEANER_INTERVAL_IN_MINUTES_KEY = "state.store.cleaner.interval.minutes";
  private static final String DEFAULT_STATE_STORE_CLEANER_INTERVAL_IN_MINUTES = "15";

  private final ScheduledExecutorService scheduledExecutor;
  private final Path stateStoreRootDir;
  private final long retention;
  private final TimeUnit retentionTimeUnit;
  private final ExecutorService cleanerRunnerExecutor;
  private final FileSystem fs;
  private final long cleanerIntervalInMinutes;

  public StateStoreCleaner(Properties properties) throws IOException {
    Preconditions.checkArgument(properties.containsKey(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY),
        "Missing configuration property for the state store root directory: "
            + ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY);

    this.stateStoreRootDir = new Path(properties.getProperty(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY));
    this.retention = Long.parseLong(properties.getProperty(
        STATE_STORE_CLEANER_RETENTION_KEY, DEFAULT_STATE_STORE_CLEANER_RETENTION));
    this.retentionTimeUnit = TimeUnit.valueOf(properties.getProperty(STATE_STORE_CLEANER_RETENTION_TIMEUNIT_KEY,
        DEFAULT_STATE_STORE_CLEANER_RETENTION_TIMEUNIT).toUpperCase());

    this.cleanerIntervalInMinutes = Long.parseLong(properties.getProperty(
            STATE_STORE_CLEANER_INTERVAL_IN_MINUTES_KEY, DEFAULT_STATE_STORE_CLEANER_INTERVAL_IN_MINUTES));
    this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor(
        ExecutorsUtils.newThreadFactory(Optional.of(LOGGER), Optional.of("StateStoreCleanerScheduler")));

    this.cleanerRunnerExecutor = Executors.newFixedThreadPool(
        Integer.parseInt(properties.getProperty(
            STATE_STORE_CLEANER_EXECUTOR_THREADS_KEY, DEFAULT_STATE_STORE_CLEANER_EXECUTOR_THREADS)),
        ExecutorsUtils.newThreadFactory(Optional.of(LOGGER), Optional.of("StateStoreCleaner")));

    URI fsUri = URI.create(properties.getProperty(
        ConfigurationKeys.STATE_STORE_FS_URI_KEY, ConfigurationKeys.LOCAL_FS_URI));
    this.fs = FileSystem.get(fsUri, HadoopUtils.getConfFromProperties(properties));
  }

  @Override
  protected void startUp() throws Exception {
    log.info("Starting the " + StateStoreCleaner.class.getSimpleName());
    log.info(String.format("Scheduling the state store cleaner task with an interval of %d minute(s)",
        this.cleanerIntervalInMinutes));

    this.scheduledExecutor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        try {
          StateStoreCleaner.this.run();
        } catch (IOException | ExecutionException e) {
          log.error("Failed to cleanup state store", e);
        }
      }
    }, 0, this.cleanerIntervalInMinutes, TimeUnit.MINUTES);
  }

  @Override
  protected void shutDown() throws Exception {
    this.close();
  }

  /**
   * Run the cleaner.
   * @throws ExecutionException
   */
  public void run() throws IOException, ExecutionException {
    if (this.fs.exists(this.stateStoreRootDir)) {
      FileStatus[] stateStoreDirs = this.fs.listStatus(this.stateStoreRootDir);
      if (stateStoreDirs == null || stateStoreDirs.length == 0) {
        log.warn("The state store root directory does not exist or is empty");
        return;
      }

      List<Future<?>> futures = Lists.newArrayList();

      for (FileStatus stateStoreDir : stateStoreDirs) {
        futures.add(this.cleanerRunnerExecutor.submit(new CleanerRunner(this.fs, stateStoreDir.getPath(), this.retention,
            this.retentionTimeUnit)));
      }

      for (Future<?> future : futures) {
        try {
          future.get();
        } catch (InterruptedException e) {
          throw new ExecutionException("Thread interrupted", e);
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
      LOGGER.info("Closing the " + StateStoreCleaner.class.getSimpleName());

      ExecutorsUtils.shutdownExecutorService(this.scheduledExecutor, Optional.of(LOGGER), 60, TimeUnit.SECONDS);
      ExecutorsUtils.shutdownExecutorService(this.cleanerRunnerExecutor, Optional.of(LOGGER), 60, TimeUnit.SECONDS);
  }

  private static class StateStoreFileFilter implements PathFilter {

    @Override
    public boolean accept(Path path) {
      String extension = Files.getFileExtension(path.getName());
      return extension.equalsIgnoreCase("jst") || extension.equalsIgnoreCase("tst");
    }
  }

  private static class CleanerRunner extends GobblinRunnable {

    private final FileSystem fs;
    private final Path stateStoreDir;
    private final long retention;
    private final TimeUnit retentionTimeUnit;

    CleanerRunner(FileSystem fs, Path stateStoreDir, long retention, TimeUnit retentionTimeUnit) {
      this.fs = fs;
      this.stateStoreDir = stateStoreDir;
      this.retention = retention;
      this.retentionTimeUnit = retentionTimeUnit;
    }

    @Override
    public void runImpl() {
      try {
        FileStatus[] stateStoreFiles = this.fs.listStatus(this.stateStoreDir, new StateStoreFileFilter());
        if (stateStoreFiles == null || stateStoreFiles.length == 0) {
          log.warn("No state store files found in directory: " + this.stateStoreDir);
          return;
        }

        log.info("Cleaning up state store directory: " + this.stateStoreDir);
        for (FileStatus file : stateStoreFiles) {
          if (shouldCleanUp(file) && !this.fs.delete(file.getPath(), false)) {
            log.error("Failed to delete state store file: " + file.getPath());
          }
        }
      } catch (IOException ioe) {
        log.error("Failed to run state store cleaner for directory: " + this.stateStoreDir, ioe);
      }
    }

    private boolean shouldCleanUp(FileStatus file) {
      DateTime now = new DateTime();
      DateTime modificationDateTime = new DateTime(file.getModificationTime());
      long retentionInMills = this.retentionTimeUnit.toMillis(this.retention);
      return modificationDateTime.plus(retentionInMills).isBefore(now);
    }
  }

  public static void main(String[] args) throws IOException {
    if (args.length != 1) {
      System.err.println("Usage: " + StateStoreCleaner.class.getSimpleName() + " <configuration file>");
      System.exit(1);
    }

    Closer closer = Closer.create();
    try {
      Properties properties = new Properties();
      properties.load(closer.register(new FileInputStream(args[0])));
      closer.register(new StateStoreCleaner(properties)).run();
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }
  }
}
