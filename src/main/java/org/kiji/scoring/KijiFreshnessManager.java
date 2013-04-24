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

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiMetaTable;
import org.kiji.schema.KijiTableNotFoundException;
import org.kiji.schema.util.ProtocolVersion;
import org.kiji.scoring.avro.KijiFreshnessPolicyRecord;

/**
 * This class is responsible for storing, retrieving and deleting freshness policies from a Kiji's
 * metatable.
 *
 * <p>Instance of class are not thread-safe. Since this class maintains a connection to the
 * metatable clients should call {@link #close()} when finished with this class.</p>
 */
@ApiAudience.Public
@ApiStability.Experimental
public final class KijiFreshnessManager implements Closeable {
  /** The minimum freshness version supported by this version of the KijiFreshnessManager. */
  private static final ProtocolVersion MIN_FRESHNESS_RECORD_VER =
      ProtocolVersion.parse("policyrecord-0.1");
  /** Maximum freshness version supported by this version of the KijiFreshnessManager. */
  private static final ProtocolVersion MAX_FRESHNESS_RECORD_VER =
      ProtocolVersion.parse("policyrecord-0.1");
  /** The freshness version that will be installed by this version of the KijiFreshnessManager. */
  private static final ProtocolVersion CUR_FRESHNESS_RECORD_VER = MAX_FRESHNESS_RECORD_VER;

  /** The prefix we use for freshness policies stored in a meta table. */
  private static final String METATABLE_KEY_PREFIX = "kiji.scoring.fresh.";

  /** The backing metatable. */
  private KijiMetaTable mMetaTable;

  /** An output stream writer suitable for serializing FreshnessPolicyRecords. */
  private final ByteArrayOutputStream mOutputStream;
  /** A datum writer for records. */
  private final DatumWriter<KijiFreshnessPolicyRecord> mRecordWriter;
  /** An encoder factory for serializing records. */
  private final EncoderFactory mEncoderFactory;
  /** An input stream reader suitable for deserializing FreshnessPolicyRecords. */
  /** A datum reader for records. */
  private final DatumReader<KijiFreshnessPolicyRecord> mRecordReader;
  /** A decoder factory for records. */
  private final DecoderFactory mDecoderFactory;

  /**
   * Default constructor.
   *
   * @param kiji a Kiji instance containing the metatable storing freshness information.
   * @throws IOException if there is an error retrieving the metatable.
   */
  private KijiFreshnessManager(Kiji kiji) throws IOException {
    mMetaTable = kiji.getMetaTable();
    // Setup members responsible for serializing/deserializing records.
    mOutputStream = new ByteArrayOutputStream();
    mRecordWriter =
        new SpecificDatumWriter<KijiFreshnessPolicyRecord>(KijiFreshnessPolicyRecord.SCHEMA$);
    mRecordReader =
        new SpecificDatumReader<KijiFreshnessPolicyRecord>(KijiFreshnessPolicyRecord.SCHEMA$);
    mEncoderFactory = EncoderFactory.get();
    mDecoderFactory = DecoderFactory.get();
  }

  /**
   * Create a new KijiFreshnessManager for the given Kiji instance.
   *
   * @param kiji the Kiji instance for which to create a freshness manager.
   * @return a new KijiFreshnessManager.
   * @throws IOException in case of an error reading from the KijiMetaTable.
   */
  public static KijiFreshnessManager create(Kiji kiji) throws IOException {
    return new KijiFreshnessManager(kiji);
  }

  /**
   * Saves a freshness policy in the metatable.
   *
   * @param tableName the table name with which the freshness policy should be associated. If the
   * table doesn't exist, an IOException will be thrown.
   * @param columnName the name of the column to associate the freshness policy with. This may be
   * either in the format of "family:qualifier" for a single column or "family" for an entire
   * map family.
   * @param producerClass class of the producer to run if the data in the column is not fresh.
   * @param policy an instance of a KijiFreshnessPolicy that will manage freshening behavior.
   * @throws IOException if there is an error saving the freshness policy or if the table is not
   * found.
   */
  public void storePolicy(String tableName, String columnName,
      Class<? extends KijiProducer> producerClass, KijiFreshnessPolicy policy)
      throws IOException {
    if (!mMetaTable.tableExists(tableName)) {
      throw new KijiTableNotFoundException("Couldn't find table: " + tableName);
    }
    // This code will throw an invalid name if there's something wrong with this columnName string.
    KijiColumnName kcn = new KijiColumnName(columnName);
    //TODO(Scoring-10): Check the column name against the current version of the table.
    KijiFreshnessPolicyRecord record = KijiFreshnessPolicyRecord.newBuilder()
        .setRecordVersion(CUR_FRESHNESS_RECORD_VER.toCanonicalString())
        .setProducerClass(producerClass.getName())
        .setFreshnessPolicyClass(policy.getClass().getName())
        .setFreshnessPolicyState(policy.serialize())
        .build();

    mOutputStream.reset();
    Encoder encoder = mEncoderFactory.directBinaryEncoder(mOutputStream, null);
    mRecordWriter.write(record, encoder);
    mMetaTable.putValue(tableName, getMetaTableKey(columnName),
        mOutputStream.toByteArray());
  }

  /**
   * Retrieves a freshness policy record for a tablename and column. Will return null if there is
   * no freshness policy registered for that column.
   *
   * @param tableName the table name.
   * @param columnName the column name, represented as a String in the form of "family:qualifier" or
   * "family" for a map family.
   * @return an avro KijiFreshnessPolicyRecord.
   * @throws IOException if an error occurs while reading from the metatable.
   */
  public KijiFreshnessPolicyRecord retrievePolicy(
      String tableName, String columnName) throws IOException {
    final byte[] recordBytes = mMetaTable.getValue(tableName, getMetaTableKey(columnName));
    Decoder decoder = mDecoderFactory.binaryDecoder(recordBytes, null);
    return mRecordReader.read(null, decoder);
  }

  /**
   * Retrieves all freshness policy records for a table.  Will return null if there are no freshness
   * policies registered for that table.
   *
   * @param tableName the table name.
   * @return a Map from KijiColumnNames to attached KijiFreshnessPolicyRecords.
   * @throws IOException if an error occurs while reading from the metatable.
   */
  public Map<KijiColumnName, KijiFreshnessPolicyRecord> retrievePolicies(String tableName)
      throws IOException {
    final Set<String> keySet = mMetaTable.keySet(tableName);
    final Map<KijiColumnName, KijiFreshnessPolicyRecord> records =
        new HashMap<KijiColumnName, KijiFreshnessPolicyRecord>();
    for (String key: keySet) {
      if (key.startsWith(METATABLE_KEY_PREFIX)) {
        final String columnName = key.substring(METATABLE_KEY_PREFIX.length());
        records.put(new KijiColumnName(columnName), retrievePolicy(tableName, columnName));
      }
    }
    return records;
  }

  /**
   * Unregisters a policy.
   *
   * @param tableName the table name.
   * @param columnName The column name, represented as a String in the form of "family:qualifier" or
   * "family" for a map family.
   * @throws IOException if an error occurs while deregistering in the metatable.
   */
  public void removePolicy(String tableName, String columnName) throws IOException {
    mMetaTable.removeValues(tableName, getMetaTableKey(columnName));
  }

  /**
   * Closes the manager, freeing resources.
   *
   * @throws IOException if an error occurs.
   */
  public void close() throws IOException {
    mMetaTable.close();
  }

  /**
   * Helper method that constructs a meta table key for a column name.
   *
   * @param columnName the column for which to get a MetaTable key.
   * @return the MetaTable key for the given column.
   */
  private String getMetaTableKey(String columnName) {
    return METATABLE_KEY_PREFIX + columnName;
  }
}
