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

package gobblin.source.extractor.filebased;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.extract.AbstractSource;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.State;
import gobblin.source.workunit.Extract;
import gobblin.source.workunit.WorkUnit;
import gobblin.source.workunit.Extract.TableType;


/**
 * This class is a base class for file based sources, it provides default
 * functionality for keeping track of which files have already been pulled
 * by the framework and for determining which files need to be pulled in this run
 * @author stakiar
 */
public abstract class FileBasedSource<S, D> extends AbstractSource<S, D> {
  private static final Logger log = LoggerFactory.getLogger(FileBasedSource.class);
  protected TimestampAwareFileBasedHelper fsHelper;
  private String splitPattern = ":::";

  /**
   * Initialize the logger.
   *
   * @param state Source state
   */
  protected void initLogger(SourceState state) {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    sb.append(Strings.nullToEmpty(state.getProp(ConfigurationKeys.SOURCE_ENTITY)));
    sb.append("]");
    MDC.put("sourceInfo", sb.toString());
  }

  /**
   * This method takes the snapshot seen in the previous run, and compares it to the list
   * of files currently in the source - it then decided which files it needs to pull
   * and distributes those files across the workunits; it does this comparison by comparing
   * the names of the files currently in the source vs. the names retrieved from the
   * previous state
   * @param state is the source state
   * @return a list of workunits for the framework to run
   */
  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    initLogger(state);
    try {
      initFileSystemHelper(state);
    } catch (FileBasedHelperException e) {
      Throwables.propagate(e);
    }

    log.info("Getting work units");
    String nameSpaceName = state.getProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY);
    String entityName = state.getProp(ConfigurationKeys.SOURCE_ENTITY);

    // Override extract table name
    String extractTableName = state.getProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY);

    // If extract table name is not found then consider entity name as extract table name
    if (Strings.isNullOrEmpty(extractTableName)) {
      extractTableName = entityName;
    }

    TableType tableType = TableType.valueOf(state.getProp(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY).toUpperCase());
    Set<String> prevFsSnapshot = getPrevFsSnapshot(state);
    List<WorkUnit> workUnits = Lists.newArrayList();
    List<WorkUnit> previousWorkUnits = this.getPreviousWorkUnitsForRetry(state);
    log.info("Total number of work units from the previous failed runs: " + previousWorkUnits.size());
    for (WorkUnit previousWorkUnit : previousWorkUnits) {
      prevFsSnapshot.addAll(previousWorkUnit.getPropAsSet(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL));
      workUnits.add(previousWorkUnit);
    }

    // Get list of files that need to be pulled
    List<String> currentFsSnapshot = this.getcurrentFsSnapshot(state);
    HashSet<String> filesWithTimeToPull = new HashSet<String>(currentFsSnapshot);
    filesWithTimeToPull.removeAll(prevFsSnapshot);
    List<String> filesToPull = new ArrayList<String>();
    Iterator<String> it = filesWithTimeToPull.iterator();
    while (it.hasNext()) {
      String filesWithoutTimeToPull[] = it.next().split(splitPattern);
      filesToPull.add(filesWithoutTimeToPull[0]);
    }

    if (!filesToPull.isEmpty()) {

      logFilesToPull(filesToPull);

      int numPartitions = state.contains(ConfigurationKeys.SOURCE_MAX_NUMBER_OF_PARTITIONS)
          && state.getPropAsInt(ConfigurationKeys.SOURCE_MAX_NUMBER_OF_PARTITIONS) <= filesToPull.size() ? state
          .getPropAsInt(ConfigurationKeys.SOURCE_MAX_NUMBER_OF_PARTITIONS) : filesToPull.size();
      if (numPartitions <= 0) {
        throw new IllegalArgumentException("The number of partitions should be positive");
      }

      int filesPerPartition = filesToPull.size() % numPartitions == 0 ?
          filesToPull.size() / numPartitions : filesToPull.size() / numPartitions + 1;

      int workUnitCount = 0;

      // Distribute the files across the workunits
      for (int fileOffset = 0; fileOffset < filesToPull.size(); fileOffset += filesPerPartition) {
        SourceState partitionState = new SourceState();
        partitionState.addAll(state);

        // Eventually these setters should be integrated with framework support for generalized watermark handling
        partitionState.setProp(ConfigurationKeys.SOURCE_FILEBASED_FS_SNAPSHOT,
                StringUtils.join(currentFsSnapshot, ","));

        List<String> partitionFilesToPull = filesToPull.subList(fileOffset,
            fileOffset + filesPerPartition > filesToPull.size() ? filesToPull.size() : fileOffset + filesPerPartition);
        partitionState
            .setProp(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL, StringUtils.join(partitionFilesToPull, ","));
        if (state.getPropAsBoolean(ConfigurationKeys.SOURCE_FILEBASED_PRESERVE_FILE_NAME, false)) {
          if (partitionFilesToPull.size() != 1) {
            throw new RuntimeException("Cannot preserve the file name if a workunit is given multiple files");
          }
          partitionState.setProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR,
              partitionState.getProp(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL));
        }

        // Use extract table name to create extract
        Extract extract = partitionState.createExtract(tableType, nameSpaceName, extractTableName);
        workUnits.add(partitionState.createWorkUnit(extract));
        workUnitCount++;
      }

      log.info("Total number of work units for the current run: " + (workUnits.size() - previousWorkUnits.size()));
    }

    return workUnits;
  }

  /**
   * This method is responsible for connecting to the source and taking
   * a snapshot of the folder where the data is present, it then returns
   * a list of the files in String format
   * @param state is used to connect to the source
   * @return a list of file name or paths present on the external data
   * directory
   */
  public List<String> getcurrentFsSnapshot(State state) {
    List<String> results = new ArrayList<String>();
    String path = state.getProp(ConfigurationKeys.SOURCE_FILEBASED_DATA_DIRECTORY) + "/*" + state
        .getProp(ConfigurationKeys.SOURCE_ENTITY) + "*";

    try {
      log.info("Running ls command with input " + path);
      results = this.fsHelper.ls(path);
      for (int i = 0; i < results.size(); i++) {
        String filePath = state.getProp(ConfigurationKeys.SOURCE_FILEBASED_DATA_DIRECTORY) + "/" + results.get(i);
        results.set(i, filePath + splitPattern + this.fsHelper.getFileMTime(filePath));
      }
    } catch (FileBasedHelperException e) {
      log.error("Not able to fetch the filename/file modified time to " + e.getMessage() + " will not pull any files",
          e);
    }
    return results;
  }

  @Override
  public void shutdown(SourceState state) {
    if (this.fsHelper != null) {
      log.info("Shutting down the FileSystemHelper connection");
      try {
        this.fsHelper.close();
      } catch (FileBasedHelperException e) {
        log.error("Unable to shutdown FileSystemHelper", e);
      }
    }
  }

  public abstract void initFileSystemHelper(State state)
      throws FileBasedHelperException;

  private void logFilesToPull(List<String> filesToPull) {
    int filesToLog = Math.min(2000, filesToPull.size());
    String remainingString = "";
    if (filesToLog < filesToPull.size()) {
      remainingString = "and " + (filesToPull.size() - filesToLog) + " more ";
    }
    log.info(String.format("Will pull the following files %sin this run: %s", remainingString,
            Arrays.toString(filesToPull.subList(0, filesToLog).toArray())));
  }

  private Set<String> getPrevFsSnapshot(SourceState state) {
    Iterable<WorkUnitState> previousWorkUnitStates = state.getPreviousWorkUnitStates();
    WorkUnitState previousWorkUnitState = Iterables.get(previousWorkUnitStates, 0, null);
    if (previousWorkUnitState != null &&
        previousWorkUnitState.getWorkunit().contains(ConfigurationKeys.SOURCE_FILEBASED_FS_SNAPSHOT)) {
      return previousWorkUnitState.getWorkunit().getPropAsSet(ConfigurationKeys.SOURCE_FILEBASED_FS_SNAPSHOT);
    }
    SourceState previousSourceState = state.getPreviousSourceState();
    if (previousSourceState != null) {
      if (previousSourceState.contains(ConfigurationKeys.SOURCE_FILEBASED_FS_SNAPSHOT)) {
        return previousSourceState.getPropAsSet(ConfigurationKeys.SOURCE_FILEBASED_FS_SNAPSHOT);
      }
      if (state.getPropAsBoolean(ConfigurationKeys.SOURCE_FILEBASED_FS_PRIOR_SNAPSHOT_REQUIRED,
          ConfigurationKeys.DEFAULT_SOURCE_FILEBASED_FS_PRIOR_SNAPSHOT_REQUIRED)) {
        // If a previous job exists, there should be a snapshot property.  If not, we need to fail so that we
        // don't accidentally read files that have already been processed.
        throw new RuntimeException(String.format("No '%s' found on state of prior job",
            ConfigurationKeys.SOURCE_FILEBASED_FS_SNAPSHOT));
      }
    }
    return Sets.newHashSet();
  }
}
