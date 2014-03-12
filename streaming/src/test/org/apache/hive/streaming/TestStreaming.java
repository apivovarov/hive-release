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

import junit.framework.Assert;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class TestStreaming {

  private static final String COL1 = "id";
  private static final String COL2 = "msg";

  private final HiveConf conf;
  private final IMetaStoreClient msClient;
  private final Hive hive;

  //public boolean local = false;
  //private final int port ;
  final String metaStoreURI ;

  private final static String user = "roshan";
  private final static String dbName = "testing";
  private final static String tblName = "alerts";
  private final static String[] fieldNames = new String[]{COL1,COL2};;

  private final String PART1_CONTINENT = "Asia";
  private final String PART1_COUNTRY = "India";

  List<String> partitionVals;
  private static String partLocation;

  //private Driver driver;

  public TestStreaming() throws Exception {
    partitionVals = new ArrayList<String>(2);
    partitionVals.add(PART1_CONTINENT);
    partitionVals.add(PART1_COUNTRY);

        /*
    if(local) {
      port = MetaStoreUtils.findFreePort();
      metaStoreURI = "thrift://localhost:" + port;
    } else {
      port = 9083;
      metaStoreURI = "thrift://172.16.0.21:" + port;
    }
    */
    //port = MetaStoreUtils.findFreePort();
    //metaStoreURI = "thrift://localhost:" + port;
    metaStoreURI = null;

    conf = new HiveConf(this.getClass());
    TxnDbUtil.setConfValues(conf);
    //conf.setVar(HiveConf.ConfVars.METASTOREURIS, metaStoreURI);

        /*
    if(local) {
      //1) Start from a clean slate (metastore)
      TxnDbUtil.cleanDb();
      TxnDbUtil.prepDb();
      */
    //1) Start from a clean slate (metastore)
    TxnDbUtil.cleanDb();
    TxnDbUtil.prepDb();

    //2) Start Hive Metastore on separate thread
    //MetaStoreUtils.startMetaStore(port, ShimLoader.getHadoopThriftAuthBridge(), conf);

    //3) obtain metastore clients
    hive = Hive.get(conf);
    msClient = hive.getMSC();
    //SessionState.start(new CliSessionState(conf));
    //driver = new Driver(conf);
  }

  @Before
  public void setup() throws Exception {
    // drop and recreate the necessary databases and tables
    dropDB(msClient, dbName);
    createDbAndTable(msClient, dbName, tblName, partitionVals);
  }

  private void printResults(ArrayList<String> res) {
    for(String s: res) {
      System.out.println(s);
    }
    System.out.println("Total records: " + res.size());
  }

  private static List<FieldSchema> getPartitionKeys() {
    List<FieldSchema> fields = new ArrayList<FieldSchema>();
    // Defining partition names in unsorted order
    fields.add(new FieldSchema("continent", serdeConstants.STRING_TYPE_NAME, ""));
    fields.add(new FieldSchema("country", serdeConstants.STRING_TYPE_NAME, ""));
    return fields;
  }

  private void checkDataWritten(long minTxn, long maxTxn, int buckets, int numExpectedFiles,
                                String... records) throws Exception {
    ValidTxnList txns = msClient.getValidTxns();
    AcidUtils.Directory dir = AcidUtils.getAcidState(new Path(partLocation), conf, txns);
    Assert.assertEquals(0, dir.getObsolete().size());
    Assert.assertEquals(0, dir.getOriginalFiles().size());
    List<AcidUtils.ParsedDelta> current = dir.getCurrentDirectories();
    System.out.println("Files found: ");
    for (AcidUtils.ParsedDelta pd : current) System.out.println(pd.getPath().toString());
    Assert.assertEquals(numExpectedFiles, current.size());

    // find the absolute mininum transaction
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    for (AcidUtils.ParsedDelta pd : current) {
      if (pd.getMaxTransaction() > max) max = pd.getMaxTransaction();
      if (pd.getMinTransaction() < min) min = pd.getMinTransaction();
    }
    Assert.assertEquals(minTxn, min);
    Assert.assertEquals(maxTxn, max);

    InputFormat inf = new OrcInputFormat();
    JobConf job = new JobConf();
    job.set("mapred.input.dir", partLocation.toString());
    job.set("bucket_count", Integer.toString(buckets));
    job.set(ValidTxnList.VALID_TXNS_KEY, txns.toString());
    InputSplit[] splits = inf.getSplits(job, 1);
    Assert.assertEquals(1, splits.length);
    org.apache.hadoop.mapred.RecordReader<NullWritable, OrcStruct> rr =
            inf.getRecordReader(splits[0], job, Reporter.NULL);

    NullWritable key = rr.createKey();
    OrcStruct value = rr.createValue();
    for(int i = 0; i < records.length; i++) {
      Assert.assertEquals(true, rr.next(key, value));
      Assert.assertEquals(records[i], value.toString());
    }
    Assert.assertEquals(false, rr.next(key, value));
  }

  private void checkNothingWritten() throws Exception {
    ValidTxnList txns = msClient.getValidTxns();
    AcidUtils.Directory dir = AcidUtils.getAcidState(new Path(partLocation), conf, txns);
    Assert.assertEquals(0, dir.getObsolete().size());
    Assert.assertEquals(0, dir.getOriginalFiles().size());
    List<AcidUtils.ParsedDelta> current = dir.getCurrentDirectories();
    Assert.assertEquals(0, current.size());
  }

  @Test
  public void testEndpointConnection() throws Exception {
    HiveEndPoint endPt = new HiveEndPoint(metaStoreURI, dbName, tblName
            , partitionVals);
    StreamingConnection connection = endPt.newConnection(user, false); //shouldn't throw
    connection.close();
  }

  @Test
  public void testAddPartition() throws Exception {
    List<String> newPartVals = new ArrayList<String>(2);
    newPartVals.add(PART1_CONTINENT);
    newPartVals.add("Nepal");

    HiveEndPoint endPt = new HiveEndPoint(metaStoreURI, dbName, tblName
            , newPartVals);

    // Ensure partition is absent
    try {
      msClient.getPartition(endPt.database, endPt.table, endPt.partitionVals);
      Assert.assertTrue("Partition already exists", false);
    } catch (NoSuchObjectException e) {
      // expect this exception
    }

    // Create partition
    Assert.assertNotNull(endPt.newConnection(user, true));

    // Ensure partition is present
    Partition p = msClient.getPartition(endPt.database, endPt.table, endPt.partitionVals);
    Assert.assertNotNull("Did not find added partition", p);
  }

  @Test
  public void testTransactionBatchEmptyCommit() throws Exception {
    HiveEndPoint endPt = new HiveEndPoint(metaStoreURI, dbName, tblName,
            partitionVals);
    DelimitedInputWriter writer = new DelimitedInputWriter(fieldNames,",", endPt);
    StreamingConnection connection = endPt.newConnection(user, false);

    TransactionBatch txnBatch =  connection.fetchTransactionBatch(10, writer);
    txnBatch.beginNextTransaction();
    txnBatch.commit();
    Assert.assertEquals(TransactionBatch.TxnState.COMMITTED
            , txnBatch.getCurrentTransactionState());
    txnBatch.close();
    connection.close();
  }

  @Test
  public void testTransactionBatchEmptyAbort() throws Exception {
    HiveEndPoint endPt = new HiveEndPoint(metaStoreURI, dbName, tblName,
            partitionVals);
    DelimitedInputWriter writer = new DelimitedInputWriter(fieldNames,",", endPt);
    StreamingConnection connection = endPt.newConnection(user, true);

    TransactionBatch txnBatch =  connection.fetchTransactionBatch(10, writer);
    txnBatch.beginNextTransaction();
    txnBatch.abort();
    Assert.assertEquals(TransactionBatch.TxnState.ABORTED
            , txnBatch.getCurrentTransactionState());
    txnBatch.close();
    connection.close();
  }

  @Test
  public void testTransactionBatchCommit() throws Exception {
    HiveEndPoint endPt = new HiveEndPoint(metaStoreURI, dbName, tblName,
            partitionVals);
    DelimitedInputWriter writer = new DelimitedInputWriter(fieldNames,",", endPt);
    StreamingConnection connection = endPt.newConnection(user, true);

    // 1st Txn
    TransactionBatch txnBatch =  connection.fetchTransactionBatch(10, writer);
    txnBatch.beginNextTransaction();
    Assert.assertEquals(TransactionBatch.TxnState.OPEN
            , txnBatch.getCurrentTransactionState());
    txnBatch.write("1,Hello streaming".getBytes());
    txnBatch.commit();

    checkDataWritten(1, 10, 1, 1, "{1, Hello streaming}");

    Assert.assertEquals(TransactionBatch.TxnState.COMMITTED
            , txnBatch.getCurrentTransactionState());

    // 2nd Txn
    txnBatch.beginNextTransaction();
    Assert.assertEquals(TransactionBatch.TxnState.OPEN
            , txnBatch.getCurrentTransactionState());
    txnBatch.write("2,Welcome to streaming".getBytes());

    // data should not be visible
    checkDataWritten(1, 10, 1, 1, "{1, Hello streaming}");

    txnBatch.commit();

    checkDataWritten(1, 10, 1, 1, "{1, Hello streaming}",
        "{2, Welcome to streaming}");

    txnBatch.close();
    Assert.assertEquals(TransactionBatch.TxnState.INACTIVE
            , txnBatch.getCurrentTransactionState());


    connection.close();
  }

  @Test
  public void testRemainingTransactions() throws Exception {
    HiveEndPoint endPt = new HiveEndPoint(metaStoreURI, dbName, tblName,
            partitionVals);
    DelimitedInputWriter writer = new DelimitedInputWriter(fieldNames,",", endPt);
    StreamingConnection connection = endPt.newConnection(user, true);

    // 1) test with txn.Commit()
    TransactionBatch txnBatch =  connection.fetchTransactionBatch(10, writer);
    int batch=0;
    int initialCount = txnBatch.remainingTransactions();
    while(txnBatch.remainingTransactions()>0) {
      txnBatch.beginNextTransaction();
      Assert.assertEquals(--initialCount, txnBatch.remainingTransactions());
      for (int rec=0; rec<2; ++rec) {
        Assert.assertEquals(TransactionBatch.TxnState.OPEN
                , txnBatch.getCurrentTransactionState());
        txnBatch.write((batch * rec + ",Hello streaming").getBytes());
      }
      txnBatch.commit();
      Assert.assertEquals(TransactionBatch.TxnState.COMMITTED
              , txnBatch.getCurrentTransactionState());
      ++batch;
    }
    Assert.assertEquals(0,txnBatch.remainingTransactions());
    txnBatch.close();

    Assert.assertEquals(TransactionBatch.TxnState.INACTIVE
            , txnBatch.getCurrentTransactionState());

    // 2) test with txn.Abort()
    txnBatch =  connection.fetchTransactionBatch(10, writer);
    batch=0;
    initialCount = txnBatch.remainingTransactions();
    while(txnBatch.remainingTransactions()>0) {
      txnBatch.beginNextTransaction();
      Assert.assertEquals(--initialCount,txnBatch.remainingTransactions());
      for (int rec=0; rec<2; ++rec) {
        Assert.assertEquals(TransactionBatch.TxnState.OPEN
                , txnBatch.getCurrentTransactionState());
        txnBatch.write((batch * rec + ",Hello streaming").getBytes());
      }
      txnBatch.abort();
      Assert.assertEquals(TransactionBatch.TxnState.ABORTED
              , txnBatch.getCurrentTransactionState());
      ++batch;
    }
    Assert.assertEquals(0,txnBatch.remainingTransactions());
    txnBatch.close();

    Assert.assertEquals(TransactionBatch.TxnState.INACTIVE
            , txnBatch.getCurrentTransactionState());

    connection.close();
  }

  @Test
  public void testTransactionBatchAbort() throws Exception {

    HiveEndPoint endPt = new HiveEndPoint(metaStoreURI, dbName, tblName,
            partitionVals);
    StreamingConnection connection = endPt.newConnection(user, false);
    DelimitedInputWriter writer = new DelimitedInputWriter(fieldNames,",", endPt);

    TransactionBatch txnBatch =  connection.fetchTransactionBatch(10, writer);
    txnBatch.beginNextTransaction();
    txnBatch.write("1,Hello streaming".getBytes());
    txnBatch.write("2,Welcome to streaming".getBytes());
    txnBatch.abort();

    checkNothingWritten();

    Assert.assertEquals(TransactionBatch.TxnState.ABORTED
            , txnBatch.getCurrentTransactionState());

    txnBatch.close();
    connection.close();

    checkNothingWritten();

  }


  @Test
  public void testTransactionBatchAbortAndCommit() throws Exception {

    HiveEndPoint endPt = new HiveEndPoint(metaStoreURI, dbName, tblName,
            partitionVals);
    StreamingConnection connection = endPt.newConnection(user, false);
    DelimitedInputWriter writer = new DelimitedInputWriter(fieldNames,",", endPt);

    TransactionBatch txnBatch =  connection.fetchTransactionBatch(10, writer);
    txnBatch.beginNextTransaction();
    txnBatch.write("1,Hello streaming".getBytes());
    txnBatch.write("2,Welcome to streaming".getBytes());
    txnBatch.abort();

    checkNothingWritten();

    Assert.assertEquals(TransactionBatch.TxnState.ABORTED
            , txnBatch.getCurrentTransactionState());

    txnBatch.beginNextTransaction();
    txnBatch.write("1,Hello streaming".getBytes());
    txnBatch.write("2,Welcome to streaming".getBytes());
    txnBatch.commit();

    checkDataWritten(1, 10, 1, 1, "{1, Hello streaming}",
        "{2, Welcome to streaming}");

    txnBatch.close();
    connection.close();
  }

  @Test
  public void testMultipleTransactionBatchCommits() throws Exception {
    HiveEndPoint endPt = new HiveEndPoint(metaStoreURI, dbName, tblName,
            partitionVals);
    DelimitedInputWriter writer = new DelimitedInputWriter(fieldNames,",", endPt);
    StreamingConnection connection = endPt.newConnection(user, false);

    TransactionBatch txnBatch =  connection.fetchTransactionBatch(10, writer);
    txnBatch.beginNextTransaction();
    txnBatch.write("1,Hello streaming".getBytes());
    txnBatch.commit();

    checkDataWritten(1, 10, 1, 1, "{1, Hello streaming}");

    txnBatch.beginNextTransaction();
    txnBatch.write("2,Welcome to streaming".getBytes());
    txnBatch.commit();

    checkDataWritten(1, 10, 1, 1, "{1, Hello streaming}",
        "{2, Welcome to streaming}");

    txnBatch.close();

    // 2nd Txn Batch
    txnBatch =  connection.fetchTransactionBatch(10, writer);
    txnBatch.beginNextTransaction();
    txnBatch.write("3,Hello streaming - once again".getBytes());
    txnBatch.commit();

    checkDataWritten(1, 20, 1, 2, "{1, Hello streaming}",
        "{2, Welcome to streaming}", "{3, Hello streaming - once again}");

    txnBatch.beginNextTransaction();
    txnBatch.write("4,Welcome to streaming - once again".getBytes());
    txnBatch.commit();

    checkDataWritten(1, 20, 1, 2, "{1, Hello streaming}",
        "{2, Welcome to streaming}", "{3, Hello streaming - once again}",
        "{4, Welcome to streaming - once again}");

    Assert.assertEquals(TransactionBatch.TxnState.COMMITTED
            , txnBatch.getCurrentTransactionState());

    txnBatch.close();

    connection.close();
  }


  @Test
  public void testConcurrentTransactionBatchCommits() throws Exception {
    HiveEndPoint endPt = new HiveEndPoint(metaStoreURI, dbName, tblName,
            partitionVals);
    DelimitedInputWriter writer = new DelimitedInputWriter(fieldNames, ",", endPt);
    StreamingConnection connection = endPt.newConnection(user, false);

    // Acquire 1st Txn Batch
    TransactionBatch txnBatch1 =  connection.fetchTransactionBatch(10, writer);
    txnBatch1.beginNextTransaction();

    // Acquire 2nd Txn Batch
    DelimitedInputWriter writer2 = new DelimitedInputWriter(fieldNames, ",", endPt);
    TransactionBatch txnBatch2 =  connection.fetchTransactionBatch(10, writer2);
    txnBatch2.beginNextTransaction();

    // Interleaved writes to both batches
    txnBatch1.write("1,Hello streaming".getBytes());
    txnBatch2.write("3,Hello streaming - once again".getBytes());

    checkNothingWritten();

    txnBatch2.commit();

    checkDataWritten(11, 20, 1, 1, "{3, Hello streaming - once again}");

    txnBatch1.commit();

    checkDataWritten(1, 20, 1, 2, "{1, Hello streaming}", "{3, Hello streaming - once again}");

    txnBatch1.beginNextTransaction();
    txnBatch1.write("2,Welcome to streaming".getBytes());

    txnBatch2.beginNextTransaction();
    txnBatch2.write("4,Welcome to streaming - once again".getBytes());

    checkDataWritten(1, 20, 1, 2, "{1, Hello streaming}", "{3, Hello streaming - once again}");

    txnBatch1.commit();

    checkDataWritten(1, 20, 1, 2, "{1, Hello streaming}",
        "{2, Welcome to streaming}",
        "{3, Hello streaming - once again}");

    txnBatch2.commit();

    checkDataWritten(1, 20, 1, 2, "{1, Hello streaming}",
        "{2, Welcome to streaming}",
        "{3, Hello streaming - once again}",
        "{4, Welcome to streaming - once again}");

    Assert.assertEquals(TransactionBatch.TxnState.COMMITTED
            , txnBatch1.getCurrentTransactionState());
    Assert.assertEquals(TransactionBatch.TxnState.COMMITTED
            , txnBatch2.getCurrentTransactionState());

    txnBatch1.close();
    txnBatch2.close();

    connection.close();

  }

  // delete db and all tables in it
  public static void dropDB(IMetaStoreClient client, String databaseName) {
    try {
      for(String table : client.listTableNamesByFilter(databaseName, "", (short)-1)) {
        client.dropTable(databaseName, table, true, true);
      }
      client.dropDatabase(databaseName);
    } catch (TException e) {
    }

  }

  public static void createDbAndTable(IMetaStoreClient client, String databaseName,
                                      String tableName, List<String> partVals)
          throws Exception {
    Database db = new Database();
    db.setName(databaseName);
    client.createDatabase(db);

    Table tbl = new Table();
    tbl.setDbName(databaseName);
    tbl.setTableName(tableName);
    tbl.setTableType(TableType.MANAGED_TABLE.toString());
    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(getTableColumns());
    sd.setNumBuckets(1);
    tbl.setPartitionKeys(getPartitionKeys());

    tbl.setSd(sd);

    sd.setBucketCols(new ArrayList<String>(2));
    sd.setSerdeInfo(new SerDeInfo());
    sd.getSerdeInfo().setName(tbl.getTableName());
    sd.getSerdeInfo().setParameters(new HashMap<String, String>());
    sd.getSerdeInfo().getParameters().put(serdeConstants.SERIALIZATION_FORMAT, "1");

    sd.getSerdeInfo().setSerializationLib(OrcSerde.class.getName());
    sd.setInputFormat(HiveInputFormat.class.getName());
    sd.setOutputFormat(OrcOutputFormat.class.getName());

    Map<String, String> tableParams = new HashMap<String, String>();
    tbl.setParameters(tableParams);
    client.createTable(tbl);

    try {
      addPartition(client, tbl, partVals);
    } catch (AlreadyExistsException e) {
    }
    Partition createdPartition = client.getPartition(databaseName, tableName, partVals);
    partLocation = createdPartition.getSd().getLocation();
    System.out.println("Partition location is " + partLocation);
  }

  /*
  private void descPart() throws CommandNeedRetryException, IOException {
    driver.run("describe formatted " + dbName + "." + tblName);
    ArrayList<String> res = new ArrayList<String>();
    driver.getResults(res);
    printResults(res);
  }
  */


  private static void addPartition(IMetaStoreClient client, Table tbl
          , List<String> partValues)
          throws IOException, TException {
    Partition part = new Partition();
    part.setDbName(dbName);
    part.setTableName(tblName);
    part.setSd(tbl.getSd());
    part.setValues(partValues);
    client.add_partition(part);
  }

  private static List<FieldSchema> getTableColumns() {
    List<FieldSchema> fields = new ArrayList<FieldSchema>();
    fields.add(new FieldSchema(COL1, serdeConstants.INT_TYPE_NAME, ""));
    fields.add(new FieldSchema(COL2, serdeConstants.STRING_TYPE_NAME, ""));
    return fields;
  }
}

// TODO: support for unpartitioned tables
