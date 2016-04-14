/*
 * Copyright (C) 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.config.client;

import java.util.concurrent.ExecutionException;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import gobblin.config.client.api.VersionStabilityPolicy;
import gobblin.util.concurrent.GobblinCallable;

/**
 * Caches {@link ConfigClient}s for every {@link VersionStabilityPolicy}.
 */
public class ConfigClientCache {

  private static final Cache<VersionStabilityPolicy, ConfigClient> CONFIG_CLIENTS_CACHE = CacheBuilder.newBuilder()
      .maximumSize(VersionStabilityPolicy.values().length).build();

  public static ConfigClient getClient(final VersionStabilityPolicy policy) {
    try {
      return CONFIG_CLIENTS_CACHE.get(policy, new GobblinCallable<ConfigClient>() {
        @Override
        public ConfigClient callImpl() throws Exception {
          return ConfigClient.createConfigClient(policy);
        }
      });
    } catch (ExecutionException e) {
      throw new RuntimeException("Failed to get Config client", e);
    }
  }
}
