/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.streaming;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.LockComponentBuilder;
import org.apache.hadoop.hive.metastore.LockRequestBuilder;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

/**
 * Information about the hive partition to write to
 */
public class HiveEndPoint {
  public final String metaStoreUri;
  public final String database;
  public final String table;
  public final List<String> partitionVals;
  public final HiveConf conf;


  static final private Log LOG = LogFactory.getLog(HiveEndPoint.class.getName());

  public HiveEndPoint(String metaStoreUri
          , String database, String table, List<String> partitionVals) throws ConnectionError {
    this.metaStoreUri = metaStoreUri;
    this.database = database;
    this.table = table;
    this.partitionVals = partitionVals;
    this.conf = createHiveConf(metaStoreUri);
  }

  /**
   * Acquire a new connection to Metastore for streaming
   * @param user
   * @param createPartIfNotExists If the partition specified in the endpoint does not exist, it will be autocreated
   * @return
   * @throws ConnectionError
   * @throws InvalidPartition
   * @throws ClassNotFoundException
   * @throws StreamingException
   */
  public StreamingConnection newConnection(String user, boolean createPartIfNotExists)
          throws ConnectionError, InvalidPartition, ClassNotFoundException
                , StreamingException {
    return new ConnectionImpl(this, user, conf, createPartIfNotExists);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    HiveEndPoint endPoint = (HiveEndPoint) o;

    if ( database != null
            ? !database.equals(endPoint.database)
            : endPoint.database != null ) {
      return false;
    }
    if ( metaStoreUri != null
            ? !metaStoreUri.equals(endPoint.metaStoreUri)
            : endPoint.metaStoreUri != null ) {
      return false;
    }
    if (!partitionVals.equals(endPoint.partitionVals)) {
      return false;
    }
    if (table != null ? !table.equals(endPoint.table) : endPoint.table != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = metaStoreUri != null ? metaStoreUri.hashCode() : 0;
    result = 31 * result + (database != null ? database.hashCode() : 0);
    result = 31 * result + (table != null ? table.hashCode() : 0);
    result = 31 * result + partitionVals.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "HiveEndPoint{" +
            "metaStoreUri='" + metaStoreUri + '\'' +
            ", database='" + database + '\'' +
            ", table='" + table + '\'' +
            ", partitionVals=(" + partitionVals +  ") }";
  }


  private HiveConf createHiveConf(String metaStoreUri) {
    HiveConf conf = new HiveConf(this.getClass());
    conf.setVar(HiveConf.ConfVars.HIVE_TXN_MANAGER,
            "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, true);
    if(metaStoreUri!= null) {
      conf.setVar(HiveConf.ConfVars.METASTOREURIS, metaStoreUri);
    }
    return conf;
  }


  // uses embedded store if endpoint.metastoreUri is null
  private static IMetaStoreClient getMetaStoreClient(HiveEndPoint endPoint, HiveConf conf)
          throws ConnectionError {

    if(endPoint.metaStoreUri!= null) {
      conf.setVar(HiveConf.ConfVars.METASTOREURIS, endPoint.metaStoreUri);
    }

    try {
      return Hive.get(conf).getMSC();
    } catch (MetaException e) {
      throw new ConnectionError("Error connecting to Hive Metastore URI: "
              + endPoint.metaStoreUri, e);
    } catch (HiveException e) {
      throw new ConnectionError("Error connecting to Hive Metastore URI: "
              + endPoint.metaStoreUri, e);
    }
  }



  private static class ConnectionImpl implements StreamingConnection {
    private final IMetaStoreClient msClient;
    private final HiveEndPoint endPt;
    private final String user;

    /**
     *
     *
     * @param endPoint
     * @param user
     * @param conf
     * @param createPart
     * @throws ConnectionError if there is trouble connecting
     * @throws InvalidPartition if specified partition does not exist (and createPart=false)
     * @throws InvalidTable if specified table does not exist
     * @throws StreamingException
     */
    private ConnectionImpl(HiveEndPoint endPoint, String user, HiveConf conf,
                           boolean createPart)
            throws ConnectionError, InvalidPartition,
                   InvalidTable, StreamingException {
      this.user = user;
      this.endPt = endPoint;
      this.msClient = getMetaStoreClient(endPoint, conf);

      if(createPart) {
        createPartitionIfNotExists(endPoint, msClient);
      }
    }


    /**
     * Close connection
     */
    public void close() {
      msClient.close();
    }

    /**
     * Acquires a new batch of transactions from Hive.

     * @param numTransactions is a hint from client indicating how many transactions client needs.
     * @param recordWriter  Used to write record. The same writer instance can
     *                      be shared with another TransactionBatch (to the same endpoint)
     *                      only after the first TransactionBatch has been closed.
     *                      Writer will be closed when the TransactionBatch is closed.
     * @return
     * @throws ConnectionError
     * @throws InvalidPartition
     * @throws StreamingException
     */
    @Override
    public TransactionBatch fetchTransactionBatch(int numTransactions,
                                                  RecordWriter recordWriter)
            throws ConnectionError, InvalidPartition, StreamingException {
      return new TransactionBatchImpl(user, endPt, numTransactions, msClient, recordWriter);
    }

    private static boolean createPartitionIfNotExists(HiveEndPoint ep, IMetaStoreClient msClient)
            throws InvalidTable, StreamingException {

      StringBuilder partName = new StringBuilder();
      for (String p : ep.partitionVals) {
        partName.append(p);
        partName.append(':');
      }
      if(LOG.isDebugEnabled()) {
        LOG.debug("Attempting to create partition : " + ep);
      }

      Partition part = new Partition();

      try {
        Table table1 = msClient.getTable(ep.database, ep.table);
        part.setDbName(ep.database);
        part.setTableName(ep.table);
        part.setValues(ep.partitionVals);
        part.setParameters(new HashMap<String, String>());
//        Warehouse.makePartPath();
        part.setSd(table1.getSd());

      } catch (NoSuchObjectException e) {
        LOG.error("Table " + ep.database + "." + ep.table + " does not exist");
        throw new InvalidTable(ep.database, ep.table);
      } catch (TException e) {
        LOG.error("Error configuring partition object for table " + ep.database + "." + ep.table
                + ": " + e.getMessage(), e);
        throw new StreamingException("Cannot connect to table DB:"
                + ep.database + ", Table: " + ep.table, e);
      }

      try {
        msClient.add_partition(part);
        LOG.info("Partition created : " + ep);
        return true;
      } catch (AlreadyExistsException e) {
        LOG.debug("Partition already exists : " + ep);
        return false;
      } catch (TException e) {
        LOG.error("Partition creation failed: " + ep + ".  "
                + StringUtils.stringifyException(e));
        throw new StreamingException("Partition creation failed", e);
      }

    }
  } // class ConnectionImpl

  private static class TransactionBatchImpl implements TransactionBatch {
    private final List<Long> txnIds;
    private int currentTxnIndex;
    private final IMetaStoreClient msClient;
    private final RecordWriter recordWriter;
    private final String user;

    private TxnState state;
    private LockRequest lockRequest = null;
    private final HiveEndPoint endPt;

    private TransactionBatchImpl(String user, HiveEndPoint endPt, int numTxns,
                                 IMetaStoreClient msClient, RecordWriter recordWriter)
            throws StreamingException {
      try {
        this.user = user;
        this.endPt = endPt;
        this.msClient = msClient;
        this.recordWriter = recordWriter;
        this.txnIds = msClient.openTxns(user, numTxns).getTxn_ids();
        this.currentTxnIndex = -1;
        this.state = TxnState.INACTIVE;
        recordWriter.newBatch(txnIds.get(0), txnIds.get(txnIds.size()-1));
      } catch (TException e) {
        throw new ConnectionError("Unable to fetch new transaction batch", e);
      }
    }

    /**
     * Activate the next available transaction in the current transaction batch
     * @throws StreamingException
     */
    public void beginNextTransaction() throws StreamingException {
      if(currentTxnIndex >= txnIds.size())
        throw new InvalidTrasactionState("No more transactions available in" +
                " current batch");
      ++currentTxnIndex;
      lockRequest = createLockRequest(endPt, user, getCurrentTxnId());
      try {
        LockResponse res = msClient.lock(lockRequest);
        if(res.getState() != LockState.ACQUIRED) {
          throw new StreamingException("Unable to acquire partition lock");
        }
      } catch (TException e) {
        throw new StreamingException("Unable to acquire partition lock", e);
      }

      state = TxnState.OPEN;
    }

    /**
     * Get Id of currently open transaction
     * @return
     */
    public Long getCurrentTxnId() {
      return txnIds.get(currentTxnIndex);
    }

    /**
     * get state of current tramsaction
     * @return
     */
    public TxnState getCurrentTransactionState() {
      return state;
    }

    /**
     * Remaining transactions are the ones that are not committed or aborted or active.
     * Active transaction is not considered part of remaining txns.
     * @return number of transactions remaining this batch.
     */
    public int remainingTransactions() {
      if(currentTxnIndex>=0) {
        return txnIds.size() - currentTxnIndex -1;
      }
      return txnIds.size();
    }


    /**
     *  Write record using RecordWriter
     * @param record  the data to be written
     * @throws ConnectionError
     * @throws IOException
     * @throws StreamingException
     */
    @Override
    public void write(byte[] record)
            throws ConnectionError, IOException, StreamingException {
      recordWriter.write(getCurrentTxnId(), record);
    }

    /**
     *  Write records using RecordWriter
     * @param records collection of rows to be written
     * @throws ConnectionError
     * @throws IOException
     * @throws StreamingException
     */
    public void write(Collection<byte[]> records)
            throws ConnectionError, IOException, StreamingException {
      for(byte[] record : records) {
        write(record);
      }
    }

    /**
     * Commit the currently open transaction
     * @throws StreamingException
     */
    @Override
    public void commit() throws StreamingException {
      try {
        recordWriter.flush();
        msClient.commitTxn(txnIds.get(currentTxnIndex));
        state = TxnState.COMMITTED;
      } catch (NoSuchTxnException e) {
        throw new InvalidTrasactionState("Invalid transaction id : "
                + getCurrentTxnId(), e);
      } catch (TxnAbortedException e) {
        throw new InvalidTrasactionState("Aborted transaction cannot be committed"
                , e);
      } catch (TException e) {
        throw new StreamingException("Unable to commit transaction"
                + getCurrentTxnId(), e);
      }
    }

    /**
     * Abort the currently open transaction
     * @throws StreamingException
     */
    @Override
    public void abort() throws StreamingException {
      try {
        msClient.rollbackTxn(getCurrentTxnId());
        state = TxnState.ABORTED;
      } catch (NoSuchTxnException e) {
        throw new InvalidTrasactionState("Invalid transaction id : "
                + getCurrentTxnId(), e);
      } catch (TException e) {
        throw new StreamingException("Unable to abort transaction id : "
                + getCurrentTxnId(), e);
      }
    }

    /**
     * Close the TransactionBatch
     * @throws StreamingException
     */
    @Override
    public void close() throws StreamingException {
      state = TxnState.INACTIVE;
      recordWriter.closeBatch();
    }

    private static LockRequest createLockRequest(final HiveEndPoint hiveEndPoint
            , String user, long txnId)
            throws InvalidPartition {
      LockRequestBuilder rqstBuilder = new LockRequestBuilder();
      rqstBuilder.setUser(user);
      rqstBuilder.setTransactionId(txnId);

//      Warehouse.makePartName(table)
      for( String partition : hiveEndPoint.partitionVals ) {
        rqstBuilder.addLockComponent(new LockComponentBuilder()
                .setDbName(hiveEndPoint.database)
                .setTableName(hiveEndPoint.table)
                //partition = Warehouse.makePartName() : TODO
                .setPartitionName(partition )
                .setShared()
                .build());
      }
      return rqstBuilder.build();
    }
  } // class TransactionBatchImpl

}  // class HiveEndPoint

// Racing to create new partition
