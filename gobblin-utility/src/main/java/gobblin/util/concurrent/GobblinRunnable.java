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

import org.slf4j.MDC;


public abstract class GobblinRunnable implements Runnable {
  private final Map<String, String> context;
  public GobblinRunnable() {
    this.context = MDC.getCopyOfContextMap();
  }

  /**
   * When an object implementing interface <code>Runnable</code> is used
   * to create a thread, starting the thread causes the object's
   * <code>run</code> method to be called in that separately executing
   * thread.
   * <p>
   * The general contract of the method <code>run</code> is that it may
   * take any action whatsoever.
   *
   * @see     java.lang.Thread#run()
   */
  @Override
  public final void run() {
    Map<String, String> originalContext = MDC.getCopyOfContextMap();
    if (context != null) {
      MDC.setContextMap(context);
    }
    try {
      runImpl();
    } finally {
      if (originalContext != null) {
        MDC.setContextMap(originalContext);
      } else {
        MDC.clear();
      }
    }
  }

  /**
   * When an object implementing interface <code>Runnable</code> is used
   * to create a thread, starting the thread causes the object's
   * <code>run</code> method to be called in that separately executing
   * thread.
   * <p>
   * The general contract of the method <code>run</code> is that it may
   * take any action whatsoever.
   *
   * @see     java.lang.Thread#run()
   */
  protected abstract void runImpl();
}