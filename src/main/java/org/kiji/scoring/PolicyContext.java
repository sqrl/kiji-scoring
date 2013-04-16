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
package org.kiji.scoring;

import org.apache.hadoop.conf.Configuration;

import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;

/**
 * Context passed to KijiFreshnessPolicy instances to provide access to outside data.
 * TODO: Convert to an interface to hide implementation details/construction.
 */
public final class PolicyContext {

  private final Configuration mConf;
  private final KijiColumnName mAttachedColumn;
  private final KijiDataRequest mUserRequest;

  /**
   * Creates a new PolicyContext to give freshness policies access to outside data.
   *
   * @param userRequest The client data request which necessitates a freshness check.
   * @param attachedColumn The column to which the freshness policy is accepted.
   * @param conf The Configuration from the Kiji instance housing the KijiTable from which this
   *   FreshKijiTableReader reads.
   */
  public PolicyContext(
      KijiDataRequest userRequest, KijiColumnName attachedColumn, Configuration conf) {
    mUserRequest = userRequest;
    mAttachedColumn = attachedColumn;
    mConf = conf;
  }

  /**
   * @return The KijiDataRequest issued by the client for this
   */
  public KijiDataRequest getUserRequest() {
    return mUserRequest;
  }

  /**
   * @return The name of the column to which the freshness policy is attached.
   */
  public KijiColumnName getAttachedColumn() {
    return mAttachedColumn;
  }

  /**
   * @return The Configuration associated with the Kiji instance for this context.
   */
  public Configuration getConfiguration() {
    return mConf;
  }
}
