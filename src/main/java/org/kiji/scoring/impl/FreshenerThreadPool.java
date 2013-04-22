/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kiji.scoring.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;

/**
 * Singleton class providing a cached thread pool for Freshening table reads.
 */
@ApiAudience.Private
@ApiStability.Experimental
public final class FreshenerThreadPool {
  private static FreshenerThreadPool mPool;
  private final ExecutorService mExecutor;

  /** Private constructor. */
  private FreshenerThreadPool() {
    mExecutor = Executors.newCachedThreadPool();
  }

  /**
   * Gets the singleton instance, creating it if necessary.
   *
   * @return The singletone instance.
   */
  private static FreshenerThreadPool getInstance() {
    if (mPool == null) {
      synchronized (FreshenerThreadPool.class) {
        if (mPool == null) {
          mPool = new FreshenerThreadPool();
        }
      }
    }
    return mPool;
  }

  /**
   * Gets the execution engine for this singleton.
   *
   * @return The singleton's executor.
   */
  public static ExecutorService get() {
    return getInstance().mExecutor;
  }
}
