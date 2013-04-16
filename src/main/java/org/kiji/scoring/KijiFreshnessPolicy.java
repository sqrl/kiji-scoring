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

import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;

/**
 * The interface for freshness policies. Clients may implement this interface to create their own
 * freshness policies for their applications.
 *
 * TODO: Fill in a highlevel description of major methods here.
 */
public interface KijiFreshnessPolicy {
  /**
   * Tests a KijiRowData for freshness according to this policy.
   *
   * @param rowData The KijiRowData to test for freshness.
   * @param policyContext the PolicyContext object containing information about this execution
   * state.
   * @return Whether the data is fresh.
   */
  boolean isFresh(KijiRowData rowData, PolicyContext policyContext);

  /**
   * Does this freshness policy operate on the client's requested data, or should it use its own
   * custom data request?
   *
   * @return Whether to use the client's data request.
   */
  boolean shouldUseClientDataRequest();

  /**
   * Custom data request required to fulfill
   * {@link KijiFreshnessPolicy#isFresh(org.kiji.schema.KijiRowData,
   * org.kiji.scoring.impl.PolicyContext)} isFresh(KijiRowData)} if the
   * client's data request is insufficient.
   *
   * @return The custom data request.
   */
  KijiDataRequest getDataRequest();

  /**
   * Serializes any state of the freshness policy for storage in a
   * {@link org.kiji.schema.KijiMetaTable}.
   *
   * @return A string representing any required state for this freshness policy.
   */
  String store();

  /**
   * Deserializes state from a {@link org.kiji.schema.KijiMetaTable} and initializes this freshness
   * policy with that state.
   * @param policyState Serialized string retrieved from a KijiMetaTable.
   */
  void load(String policyState);
}
