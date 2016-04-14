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

package gobblin.util.concurrent;

import java.util.Map;
import java.util.concurrent.Callable;

import org.slf4j.MDC;


public abstract class GobblinCallable<V> implements Callable<V> {
  private final Map<String, String> context;

  public GobblinCallable() {
      this.context = MDC.getCopyOfContextMap();
  }

  /**
   * Computes a result, or throws an exception if unable to do so.
   *
   * @return computed result
   * @throws Exception if unable to compute a result
   */
  @Override
  public final V call() throws Exception {
    V answer = null;
    Map<String, String> originalContext = MDC.getCopyOfContextMap();
    if (context != null) {
      MDC.setContextMap(context);
    }

    try {
      answer = callImpl();
    } finally {
      if (originalContext != null) {
        MDC.setContextMap(originalContext);
      } else {
        MDC.clear();
      }
    }

    return answer;
  }

  /**
   * Computes a result, or throws an exception if unable to do so.
   *
   * @return computed result
   * @throws Exception if unable to compute a result
   */
  protected abstract V callImpl() throws Exception;
}