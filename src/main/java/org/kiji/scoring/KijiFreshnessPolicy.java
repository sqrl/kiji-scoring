// How are these constructed? How/where are they retrieved from the metatable?  What information
// should be serialized into the metatable and how?

public interface KijiFreshnessPolicy {
  // Is the RowData fresh?
  boolean isFresh(KijiRowData);

  // If the freshness policy requires a special data request, otherwise null
  KijiDataRequest requiredDataRequest();

  // serialize the freshness policy for storage in the metatable
  // can be String or Avro record?
  KijiFreshnessPolicyRecord store();

  // Deserialize a freshness policy from the metatable
  // Build the necessary state? basically a constructor?
  // Should this take a KijiDataRequest and find the freshness policies it needs based on that?
  void load(KijiFreshnessPolicyRecord);
}
