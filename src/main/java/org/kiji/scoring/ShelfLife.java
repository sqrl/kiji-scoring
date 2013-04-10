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

import java.util.NavigableSet;

import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;

/**
 * Created with IntelliJ IDEA. User: aaron Date: 4/10/13 Time: 11:01 AM To change this template use
 * File | Settings | File Templates.
 */
public final class ShelfLife implements KijiFreshnessPolicy {
  // TODO: Figure out how we get the column name
  private KijiColumnName mTargetColumnName;
  private long mShelfLifeMillis = -1;

  @Override
  public boolean isFresh(KijiRowData rowData) {
    if (mShelfLifeMillis == -1) {
      throw new RuntimeException("Shelf life not set.  Did you call ShelfLife.load()?");
    }
    if (mTargetColumnName == null) {
      throw new RuntimeException("Target column was not set.");
    }
    if (!rowData.containsColumn(mTargetColumnName.getFamily(), mTargetColumnName.getQualifier())) {
      return false;
    }
    NavigableSet<Long> timestamps =
        rowData.getTimestamps(mTargetColumnName.getFamily(), mTargetColumnName.getQualifier());
    if (timestamps.isEmpty()) {
      return false;
    }
    return System.currentTimeMillis() - timestamps.first() <= mShelfLifeMillis;
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
    // The only required state is the shelf life duration.
    return String.valueOf(mShelfLifeMillis);
  }

  @Override
  public void load(String policyState) {
    // Load the shelf life from the policy state.
    mShelfLifeMillis = Long.parseLong(policyState);
  }
}
