package org.kiji.scoring.impl;

import org.apache.hadoop.conf.Configuration;

import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;

/**
 * Context passed to KijiFreshnessPolicy instances to provide access to outside data.
 */
public final class PolicyContext {

  private final Configuration mConf;
  private final KijiColumnName mAttachedColumn;
  private final KijiDataRequest mUserRequest;

  /**
   * Creates a new PolicyContext to give freshness policies access to outside data.
   *
   * @param userRequest The client data request which necessitates a freshness check.
   * @param attachedColumn The column to which the freshness policy is accepted.
   * @param conf The Configuration from the Kiji instance housing the KijiTable from which this
   *   FreshKijiTableReader reads.
   */
  public PolicyContext(
      KijiDataRequest userRequest, KijiColumnName attachedColumn, Configuration conf) {
    mUserRequest = userRequest;
    mAttachedColumn = attachedColumn;
    mConf = conf;
  }

  /**
   * @return The KijiDataRequest issued by the client for this
   */
  public KijiDataRequest getUserRequest() {
    return mUserRequest;
  }

  /**
   * @return The name of the column to which the freshness policy is attached.
   */
  public KijiColumnName getAttachedColumn() {
    return mAttachedColumn;
  }

  /**
   * @return The Configuration associated with the Kiji instance for this context.
   */
  public Configuration getConfiguration() {
    return mConf;
  }
}
