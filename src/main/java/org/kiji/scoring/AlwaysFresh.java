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
 * A stock {@link org.kiji.scoring.KijiFreshnessPolicy} which returns fresh for any KijiRowData.
 */
public final class AlwaysFresh implements KijiFreshnessPolicy {
  //{@inheritDoc}
  @Override
  public boolean isFresh(KijiRowData rowData, PolicyContext policyContext) {
    return true;
  }

  @Override
  public boolean shouldUseClientDataRequest() {
    return true;
  }

  @Override
  public KijiDataRequest getDataRequest() {
    return null;
  }

  @Override
  public String store() {
    // Return the empty string because AlwaysFresh requires no state.
    return "";
  }

  @Override
  public void load(String policyState) {
    // empty because this policy has no state.
  }
}
