// How are these constructed? How/where are they retrieved from the metatable?  What information
// should be serialized into the metatable and how?
package org.kiji.scoring;

import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiDataRequest;

public interface KijiFreshnessPolicy {
  /**
   * Tests a KijiRowData for freshness according to this policy.
   *
   * @param rowData The KijiRowData to test for freshness.
   * @param return Whether the data is fresh.
   */
  boolean isFresh(KijiRowData rowData);

  /**
   * Does this freshness policy operate on the client's requested data, or should it use its own
   * custom data request?
   *
   * @return Whether to use the client's data request.
   */
  boolean shouldUseClientDataRequest();

  /**
   * Custom data request required to fulfill
   * {@link KijiFreshnessPolicy#isFresh(org.kiji.schema.KijiRowData)} isFresh(KijiRowData)} if the
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
