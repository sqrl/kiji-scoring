package org.kiji.scoring;

import java.io.IOException;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.HConstants;

import org.kiji.mapreduce.kvstore.KeyValueStoreReader;
import org.kiji.mapreduce.produce.ProducerContext;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableWriter;

/**
 * Created with IntelliJ IDEA. User: aaron Date: 4/16/13 Time: 4:17 PM To change this template use
 * File | Settings | File Templates.
 */
public final class KijiFreshProducerContext implements ProducerContext {

  private EntityId mEntityId;
  private String mFamily;
  private String mQualifier;
  private KijiTableWriter mWriter;

  private KijiFreshProducerContext(KijiTable table, KijiColumnName outputColumn, EntityId eid) {
    mEntityId = eid;
    mWriter = table.openTableWriter();
    mFamily = Preconditions.checkNotNull(outputColumn.getFamily());
    mQualifier = outputColumn.getQualifier();
  }

  public KijiFreshProducerContext create(
      KijiTable table, KijiColumnName outputColumn, EntityId eid) {
    return new KijiFreshProducerContext(table, outputColumn, eid);
  }

  @Override
  public EntityId getEntityId() {
    return mEntityId;
  }

  @Override
  public <T> void put(final T value) throws IOException {
    put(HConstants.LATEST_TIMESTAMP, value);
  }

  @Override
  public <T> void put(final long timestamp, final T value) throws IOException {
    mWriter.put(mEntityId, mFamily, mQualifier, timestamp, value);
  }

  @Override
  public <T> void put(final String qualifier, final T value) throws IOException {
    put(qualifier, HConstants.LATEST_TIMESTAMP, value);
  }

  @Override
  public <T> void put(final String qualifier, final long timestamp, final T value) throws IOException {
    mWriter.put(mEntityId, mFamily, qualifier, timestamp, value);
  }

  @Override
  public <K, V> KeyValueStoreReader<K, V> getStore(final String s) throws IOException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void incrementCounter(final Enum<?> anEnum) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void incrementCounter(final Enum<?> anEnum, final long l) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void progress() {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void setStatus(final String s) throws IOException {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public String getStatus() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void close() throws IOException {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void flush() throws IOException {
    //To change body of implemented methods use File | Settings | File Templates.
  }
}
