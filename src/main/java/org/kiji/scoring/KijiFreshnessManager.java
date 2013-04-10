
// Implements Closeable because it has a connection to the metatable
// Should this be a static utility class? All methods would need to take a kiji instance as well.
public final class KijiFreshnessManager implements Closeable {

  // Get a Kiji in to get the metatable
  public KijiFreshnessManager(Kiji kiji) {
    mMetaTable = kiji.getMetaTable();
  }

  public void storeFreshness(String tableName, String columnName,
      Class<T extends KijiProducer> producerClass, FreshnessPolicy policy) throws IOException {
    // write to the metatable.  Is the metatable maxversions = 1?
    // TODO how do we manage freshness policy namespace
  }

  public void removeFreshness(String tableName, String columnName) {
    // delete from the metatable by the timestamp of the old value so that a new value may be
    // written without conflict.
  }

