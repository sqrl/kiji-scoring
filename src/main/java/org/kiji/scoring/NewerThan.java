package org.kiji.scoring;

import java.util.NavigableSet;

import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.scoring.impl.PolicyContext;

/**
 * A stock {@link org.kiji.scoring.KijiFreshnessPolicy} which returns fresh if requested data was
 * modified later than a specified timestamp.
 */
public class NewerThan implements KijiFreshnessPolicy {
  private long mNewerThanTimestamp = -1;

  @Override
  public boolean isFresh(KijiRowData rowData, PolicyContext policyContext) {
    final KijiColumnName columnName = policyContext.getAttachedColumn();
    if (mNewerThanTimestamp == -1) {
      throw new RuntimeException("Newer than timestamp not set.  Did you call NewerThan.load()?");
    }
    if (columnName == null) {
      throw new RuntimeException("Target column was not set.");
    }
    // If the column does not exist in the row data, it is not fresh.
    if (!rowData.containsColumn(columnName.getFamily(), columnName.getQualifier())) {
      return false;
    }

    NavigableSet<Long> timestamps =
        rowData.getTimestamps(columnName.getFamily(), columnName.getQualifier());
    // If there are no values in the column in the row data, it is not fresh.
    if (timestamps.isEmpty()) {
      return false;
    }
    return timestamps.first() >= mNewerThanTimestamp;
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
    // The only required state is the newer than timestamp.
    return String.valueOf(mNewerThanTimestamp);
  }

  @Override
  public void load(String policyState) {
    // Load the newer than timestamp from the policy state.
    mNewerThanTimestamp = Long.parseLong(policyState);
  }
}
