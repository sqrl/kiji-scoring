// How are these constructed? How/where are they retrieved from the metatable?  What information
// should be serialized into the metatable and how?
package org.kiji.scoring;

import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiDataRequest;

public interface KijiFreshnessPolicy {
  // Is the RowData fresh?
  boolean isFresh(KijiRowData rowData);

  // Does the user's data request fulfill the requirements of isFresh?
  boolean shouldUseClientDataRequest();

  // If the freshness policy requires a special data request, otherwise null
  KijiDataRequest getDataRequest();

  // serialize any state needed by the freshness policy for storage in the metatable
  // can be String or Avro record?
  KijiFreshnessPolicyRecord store();

  // Deserialize a freshness policy's state from the metatable
  // Build the necessary state? basically a constructor?
  void load(KijiFreshnessPolicyRecord record);
}
