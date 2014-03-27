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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.RecordUpdater;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.thrift.TException;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/**
 * Uses LazySimple serde to enode the record
 */
abstract class AbstractLazySimpleRecordWriter implements RecordWriter {

  private final Path partitionPath;
  private final int  totalBuckets;

  private OrcOutputFormat outf = new OrcOutputFormat();
  private final HiveConf conf;
  private RecordUpdater updater = null;
  private final ArrayList<String> tableColumns;
  private final static String serdeClassName = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
  private LazySimpleSerDe serde;
  private ObjectInspector inspector;
  private Random rand = new Random();
  private int currentBucketId = 0;

  private char serdeSeparator;

  static final private Log LOG = LogFactory.getLog(AbstractLazySimpleRecordWriter.class.getName());

  /** Base type for handling Delimited text input. Uses LazySimpleSerde
   * to convert input byte[] to underlying Object representation
   * @param endPoint
   * @throws ConnectionError Problem talking to Hive
   * @throws ClassNotFoundException Serde class not found
   * @throws SerializationError Serde initialization/interaction failed
   * @throws StreamingException Problem acquiring file system path for partition
   */
  protected AbstractLazySimpleRecordWriter(HiveEndPoint endPoint)
          throws ConnectionError, ClassNotFoundException, SerializationError
          , StreamingException {
         this(endPoint, (char)LazySimpleSerDe.DefaultSeparators[0]);
  }

  protected AbstractLazySimpleRecordWriter(HiveEndPoint endPoint, char serdeSeparator)
          throws ConnectionError, ClassNotFoundException, SerializationError
                 , StreamingException {
    try {
      conf = HiveEndPoint.createHiveConf(this.getClass(), endPoint.metaStoreUri);

      Hive hive = Hive.get(conf, false);

      Table tbl = hive.getTable(endPoint.database, endPoint.table);
      Properties tableProps = MetaStoreUtils.getTableMetadata(tbl.getTTable());
      tableProps.setProperty("field.delim", String.valueOf(serdeSeparator));
      this.serde = createSerde(tableProps, conf);
      this.inspector = this.serde.getObjectInspector();
      this.tableColumns = getPartitionCols(tbl);
      this.partitionPath = getPathForEndPoint(hive, endPoint);
      this.totalBuckets = tbl.getNumBuckets();
      this.serdeSeparator = serdeSeparator;

    } catch (HiveException e) {
      throw new ConnectionError("Problem connecting to Hive", e);
    } catch (SerDeException e) {
      throw new SerializationError("Failed to get object inspector from Serde "
              + serdeClassName, e);
    }
  }

  private RecordUpdater createRecordUpdater(int bucketId, Long minTxnId, Long maxTxnID)
          throws IOException {
    return  outf.getRecordUpdater(partitionPath,
                 new AcidOutputFormat.Options(conf)
                    .inspector(inspector)
                    .bucket(bucketId)
                    .minimumTransactionId(minTxnId)
                    .maximumTransactionId(maxTxnID));
  }

  protected abstract byte[] reorderFields(byte[] record)
          throws UnsupportedEncodingException;

  protected ArrayList<String> getTableColumns() {
    return tableColumns;
  }

  @Override
  public void write(long transactionId, byte[] record)
          throws SerializationError, StreamingIOFailure {
    try {
      byte[] orderedFields = reorderFields(record);
      Object encodedRow = encode(orderedFields);
      updater.insert(transactionId, encodedRow);
    } catch (IOException e) {
      throw new StreamingIOFailure("Error writing record in transaction("
              + transactionId + ")", e);
    }
  }

  @Override
  public void flush() throws StreamingIOFailure {
    try {
      updater.flush();
    } catch (IOException e) {
      throw new StreamingIOFailure("Unable to flush recordUpdater", e);
    }
  }

  @Override
  public void clear() throws StreamingIOFailure {
    return;
  }

  /**
   * Creates a new record updater for the new batch
   * @param minTxnId
   * @param maxTxnID
   * @throws StreamingIOFailure if failed to create record updater
   */
  @Override
  public void newBatch(Long minTxnId, Long maxTxnID) throws StreamingIOFailure {
    try {
      this.currentBucketId = rand.nextInt(totalBuckets);
      LOG.debug("Creating Record updater");
      updater = createRecordUpdater(currentBucketId, minTxnId, maxTxnID);
    } catch (IOException e) {
      LOG.error("Failed creating record updater", e);
      throw new StreamingIOFailure("Unable to get new record Updater", e);
    }
  }

  @Override
  public void closeBatch() throws StreamingIOFailure {
    try {
      updater.close(false);
      updater = null;
    } catch (IOException e) {
      throw new StreamingIOFailure("Unable to close recordUpdater", e);
    }
  }

  private Object encode(byte[] record) throws SerializationError {
    try {
      BytesWritable blob = new BytesWritable();
      blob.set(record, 0, record.length);
      return serde.deserialize(blob);
    } catch (SerDeException e) {
      throw new SerializationError("Unable to convert byte[] record into Object", e);
    }
  }

  /**
   * Creates LazySimpleSerde
   * @param tableProps   used to create serde
   * @param conf         used to create serde
   * @return
   * @throws ClassNotFoundException if serde class not found
   * @throws SerializationError if serde could not be initialized
   */
  private static LazySimpleSerDe createSerde(Properties tableProps, HiveConf conf)
          throws ClassNotFoundException, SerializationError {
    try {
      Class<?> serdeClass = Class.forName(serdeClassName);
      LazySimpleSerDe serde =
              (LazySimpleSerDe) ReflectionUtils.newInstance(serdeClass, conf);
      serde.initialize(conf, tableProps);
      return serde;
    } catch (SerDeException e) {
      throw new SerializationError("Error initializing serde", e);
    }
  }

  private Path getPathForEndPoint(Hive hive, HiveEndPoint endPoint)
          throws StreamingException {
    try {
      IMetaStoreClient msClient = hive.getMSC();
      String location = null;
      if(endPoint.partitionVals==null || endPoint.partitionVals.isEmpty() ) {
        location = msClient.getTable(endPoint.database,endPoint.table)
                .getSd().getLocation();
      } else {
        location = msClient.getPartition(endPoint.database, endPoint.table,
                endPoint.partitionVals).getSd().getLocation();
      }
      return new Path(location);
    } catch (TException e) {
      throw new StreamingException(e.getMessage()
              + ". Unable to get path for end point: "
              + endPoint.partitionVals, e);
    }
  }

  private ArrayList<String> getPartitionCols(Table table) {
    List<FieldSchema> cols = table.getCols();
    ArrayList<String> colNames = new ArrayList<String>(cols.size());
    for(FieldSchema col : cols) {
      colNames.add(col.getName().toLowerCase());
    }
    return  colNames;
  }

  public char getSerdeSeparator() {
    return serdeSeparator;
  }
}
