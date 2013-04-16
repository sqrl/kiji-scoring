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

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiMetaTable;

/**
 * This class is responsible for storing, retrieving and deleting freshness policies from a Kiji's
 * metatable.
 *
 * <p>Since this class maintains a connection to the metatable clients should call {@link close()}
 * when finished with this class.</p>
 */
public final class KijiFreshnessManager implements Closeable {
  /** The backing metatable. */
  private KijiMetaTable mMetaTable;

  /**
   * Default constructor.
   *
   * @param kiji a Kiji instance containing the metatable storing freshness information.
   * @throws IOException if there is an error retrieving the metatable.
   */
  public KijiFreshnessManager(Kiji kiji) throws IOException {
    mMetaTable = kiji.getMetaTable();
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
      Class<KijiProducer> producerClass, KijiFreshnessPolicy policy) throws IOException {
    // write to the meta table.  Is the meta table max versions = 1?
    // TODO: this
    //mMetaTable.putValue()
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
    final byte[] record = mMetaTable.getValue(tableName, "kiji.scoring.fresh." + columnName);
    // TODO parse the record
    return null;
  }

  /**
   * Retrieves all freshness policy records for a table.  Will return null if there are no freshness
   * policies registered for that table.
   *
   * @param tableName the table name.
   * @return a List of avro KijiFreshnessPolicyRecords.
   * @throws IOException if an error occurs while reading from the metatable.
   */
  public Map<KijiColumnName, KijiFreshnessPolicyRecord> retrievePolicies(String tableName) throws IOException {
    final Set<String> keySet = mMetaTable.keySet(tableName);
    final Map<KijiColumnName, KijiFreshnessPolicyRecord> records =
        new HashMap<KijiColumnName, KijiFreshnessPolicyRecord>();
    for (String key: keySet) {
      if (key.startsWith("kiji.scoring.fresh.")) {
        final String columnName = key.substring(19);
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
    mMetaTable.removeValues(tableName, columnName);
  }

  /**
   * Closes the manager, freeing resources.
   *
   * @throws IOException if an error occurs.
   */
  public void close() throws IOException {
    mMetaTable.close();
  }
}
