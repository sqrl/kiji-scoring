package org.kiji.scoring;

import java.io.Closeable;
import java.io.IOException;

import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiMetaTable;

// Implements Closeable because it has a connection to the metatable
// Should this be a static utility class? All methods would need to take a kiji instance as well.
public final class KijiFreshnessManager implements Closeable {

  KijiMetaTable mMetaTable;

  // Get a Kiji in to get the metatable
  public KijiFreshnessManager(Kiji kiji) throws IOException {
    mMetaTable = kiji.getMetaTable();
  }

  public void storeFreshness(String tableName, String columnName,
      Class<KijiProducer> producerClass, KijiFreshnessPolicy policy) throws IOException {
    // write to the meta table.  Is the meta table max versions = 1?
    // TODO: this
    //mMetaTable.putValue()
  }

  public KijiFreshnessPolicyRecord retrievePolicy(
      String tableName, String columnName) throws IOException {
    // TODO: this
    return null;
  }

  public void removeFreshness(String tableName, String columnName) throws IOException {
    mMetaTable.removeValues(tableName, columnName);
  }

  public void close() throws IOException {
    mMetaTable.close();
  }
}
