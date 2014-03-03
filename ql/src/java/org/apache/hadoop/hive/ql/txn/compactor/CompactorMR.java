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
package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidTxnListImpl;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.*;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.*;

import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * Class to do compactions via an MR job.  This has to be in the ql package rather than metastore
 * .compactions package with all of it's relatives because it needs access to the actual input
 * and output formats, which are in ql.  ql depends on metastore and we can't have a circular
 * dependency.
 */
public class CompactorMR<V extends Writable> {

  static final private String CLASS_NAME = CompactorMR.class.getName();
  static final private Log LOG = LogFactory.getLog(CLASS_NAME);

  static final private String INPUT_FORMAT_CLASS_NAME = "hive.compactor.input.format.class.name";
  static final private String OUTPUT_FORMAT_CLASS_NAME = "hive.compactor.output.format.class.name";
  static final private String LOCATION = "hive.compactor.input.dir";
  static final private String SERDE = "hive.compactor.serde";
  static final private String MIN_TXN = "hive.compactor.txn.min";
  static final private String MAX_TXN = "hive.compactor.txn.max";
  static final private String IS_MAJOR = "hive.compactor.is.major";
  static final private String IS_COMPRESSED = "hive.compactor.is.compressed";
  static final private String TABLE_PROPS = "hive.compactor.table.props";
  static final private String NUM_BUCKETS = "hive.compactor.num.buckets";
  static final private String BASE_DIR = "hive.compactor.base.dir";
  static final private String DELTA_DIRS = "hive.compactor.delta.dirs";
  static final private String DIRS_TO_SEARCH = "hive.compactor.dirs.to.search";

  public CompactorMR() {
  }

  /**
   * Run a compactor job.
   * @param conf Hive configuration file
   * @param jobName name to run this job with
   * @param t metastore table
   * @param sd metastore storage descriptor
   * @param txns list of valid transactions
   * @param isMajor is this a major compaction?
   */
  void run(HiveConf conf, String jobName, Table t, StorageDescriptor sd,
           ValidTxnList txns, boolean isMajor) {
    try {
      JobConf job = new JobConf(conf);
      job.setJobName(jobName);
      job.setOutputKeyClass(NullWritable.class);
      job.setOutputValueClass(NullWritable.class);
      job.setJarByClass(CompactorMR.class);
      LOG.debug("User jar set to " + job.getJar());
      job.setMapperClass(CompactorMap.class);
      job.setNumReduceTasks(0);
      job.setInputFormat(CompactorInputFormat.class);
      job.setOutputFormat(NullOutputFormat.class);

      job.set(LOCATION, sd.getLocation());
      job.set(INPUT_FORMAT_CLASS_NAME, sd.getInputFormat());
      job.set(OUTPUT_FORMAT_CLASS_NAME, sd.getOutputFormat());
      job.set(SERDE, sd.getSerdeInfo().getName());
      job.setBoolean(IS_MAJOR, isMajor);
      job.setBoolean(IS_COMPRESSED, sd.isCompressed());
      job.set(TABLE_PROPS, new StringableMap(t.getParameters()).toString());
      job.setInt(NUM_BUCKETS, sd.getBucketColsSize());
      job.set(ValidTxnList.VALID_TXNS_KEY, txns.toString());

      // Figure out and encode what files we need to read.  We do this here (rather than in
      // getSplits below) because as part of this we discover our minimum and maximum transactions,
      // and discovering that in getSplits is too late as we then have no way to pass it to our
      // mapper.

      AcidUtils.Directory dir = AcidUtils.getAcidState(new Path(sd.getLocation()), conf, txns);
      StringableList dirsToSearch = new StringableList();
      Path baseDir = null;
      if (isMajor) {
        // There may not be a base dir if the partition was empty before inserts or if this
        // partition is just now being converted to ACID.
        baseDir = dir.getBaseDirectory();
        if (baseDir == null) {
          List<FileStatus> originalFiles = dir.getOriginalFiles();
          if (!(originalFiles == null) && !(originalFiles.size() == 0)) {
            // There are original format files
            for (FileStatus stat : originalFiles) {
              dirsToSearch.add(stat.getPath());
              LOG.debug("Adding original file " + stat.getPath().toString() + " to dirs to search");
            }
            // Set base to the location so that the input format reads the original files.
            baseDir = new Path(sd.getLocation());

            /*
            Path origDir = new Path(sd.getLocation());
            dirsToSearch.add(origDir);
            LOG.debug("Adding original directory " + origDir + " to dirs to search");
            */
          }
        } else {
          // add our base to the list of directories to search for files in.
          LOG.debug("Adding base directory " + baseDir + " to dirs to search");
          dirsToSearch.add(baseDir);
        }
      }

      List<AcidUtils.ParsedDelta> parsedDeltas = dir.getCurrentDirectories();

      if (parsedDeltas == null || parsedDeltas.size() == 0) {
        // Seriously, no deltas?  Can't compact that.
        LOG.error("No delta files found to compact in " + sd.getLocation());
        return;
      }

      StringableList deltaDirs = new StringableList();
      long minTxn = Long.MAX_VALUE;
      long maxTxn = Long.MIN_VALUE;
      for (AcidUtils.ParsedDelta delta : parsedDeltas) {
        LOG.debug("Adding delta " + delta.getPath() + " to directories to search");
        dirsToSearch.add(delta.getPath());
        deltaDirs.add(delta.getPath());
        minTxn = Math.min(minTxn, delta.getMinTransaction());
        maxTxn = Math.max(maxTxn, delta.getMaxTransaction());
      }

      if (baseDir != null) job.set(BASE_DIR, baseDir.toString());
      job.set(DELTA_DIRS, deltaDirs.toString());
      job.set(DIRS_TO_SEARCH, dirsToSearch.toString());
      job.setLong(MIN_TXN, minTxn);
      job.setLong(MAX_TXN, maxTxn);
      LOG.debug("Setting minimum transaction to " + minTxn);
      LOG.debug("Setting maximume transaction to " + maxTxn);

      JobClient.runJob(job);
    } catch (Throwable e) {
      // Don't let anything past us.
      LOG.error("Running MR job " + jobName + " to compact failed, " +
          StringUtils.stringifyException(e));
    }
  }

  static class CompactorInputSplit implements InputSplit {
    private long length = 0;
    private List<String> locations;
    private int bucketNum;
    private Path base;
    private Path[] deltas;

    public CompactorInputSplit() {
    }

    /**
     *
     * @param hadoopConf
     * @param bucket bucket to be processed by this split
     * @param files actual files this split should process.  It is assumed the caller has already
     *              parsed out the files in base and deltas to populate this list.
     * @param base directory of the base, or the partition/table location if the files are in old
     *             style.  Can be null.
     * @param deltas directories of the delta files.
     * @throws IOException
     */
    CompactorInputSplit(Configuration hadoopConf, int bucket, List<Path> files, Path base,
                               Path[] deltas)
        throws IOException {
      bucketNum = bucket;
      this.base = base;
      this.deltas = deltas;
      locations = new ArrayList<String>();

      for (Path path : files) {
        FileSystem fs = path.getFileSystem(hadoopConf);
        FileStatus stat = fs.getFileStatus(path);
        length += stat.getLen();
        BlockLocation[] locs = fs.getFileBlockLocations(stat, 0, length);
        for (int i = 0; i < locs.length; i++) {
          String[] hosts = locs[i].getHosts();
          for (int j = 0; j < hosts.length; j++) {
            locations.add(hosts[j]);
          }
        }
      }
    }

    @Override
    public long getLength() throws IOException {
      return length;
    }

    @Override
    public String[] getLocations() throws IOException {
      return locations.toArray(new String[locations.size()]);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      dataOutput.writeLong(length);
      dataOutput.writeInt(locations.size());
      for (int i = 0; i < locations.size(); i++) {
        dataOutput.writeInt(locations.get(i).length());
        dataOutput.writeBytes(locations.get(i));
      }
      dataOutput.writeInt(bucketNum);
      if (base == null) {
        dataOutput.writeInt(0);
      } else {
        dataOutput.writeInt(base.toString().length());
        dataOutput.writeBytes(base.toString());
      }
      dataOutput.writeInt(deltas.length);
      for (int i = 0; i < deltas.length; i++) {
        dataOutput.writeInt(deltas[i].toString().length());
        dataOutput.writeBytes(deltas[i].toString());
      }

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      int len;
      byte[] buf;

      locations = new ArrayList<String>();
      length = dataInput.readLong();
      LOG.debug("Read length of " + length);
      int numElements = dataInput.readInt();
      LOG.debug("Read numElements of " + numElements);
      for (int i = 0; i < numElements; i++) {
        len = dataInput.readInt();
        LOG.debug("Read file length of " + len);
        buf = new byte[len];
        dataInput.readFully(buf);
        locations.add(new String(buf));
      }
      bucketNum = dataInput.readInt();
      LOG.debug("Read bucket number of " + bucketNum);
      len = dataInput.readInt();
      LOG.debug("Read base path length of " + len);
      if (len > 0) {
        buf = new byte[len];
        dataInput.readFully(buf);
        base = new Path(new String(buf));
      }
      numElements = dataInput.readInt();
      deltas = new Path[numElements];
      for (int i = 0; i < numElements; i++) {
        len = dataInput.readInt();
        buf = new byte[len];
        dataInput.readFully(buf);
        deltas[i] = new Path(new String(buf));
      }
    }

    int getBucket() {
      return bucketNum;
    }

    Path getBaseDir() {
      return base;
    }

    Path[] getDeltaDirs() {
      return deltas;
    }
  }

  static class CompactorInputFormat<V extends Writable>
      implements InputFormat<RecordIdentifier, V> {

    @Override
    public InputSplit[] getSplits(JobConf entries, int i) throws IOException {
      Path baseDir = null;
      if (entries.get(BASE_DIR) != null) baseDir = new Path(entries.get(BASE_DIR));
      StringableList deltaDirs = new StringableList(entries.get(DELTA_DIRS));
      StringableList dirsToSearch = new StringableList(entries.get(DIRS_TO_SEARCH));

      InputSplit[] splits = new InputSplit[entries.getInt(NUM_BUCKETS, 1)];
      for (int bucket = 0; bucket < splits.length; bucket++) {

        // Go find the actual files to read.  This will change for each split.
        List<Path> filesToRead = new ArrayList<Path>();
        for (Path d : dirsToSearch) {
          // If this is a base or delta directory, then we need to be looking for a bucket file.
          // But if it's a legacy file then we need to add it directly.
          if (d.getName().startsWith(AcidUtils.BASE_PREFIX) ||
              d.getName().startsWith(AcidUtils.DELTA_PREFIX)) {
            Path bucketFile = AcidUtils.createBucketFile(d, bucket);
            filesToRead.add(bucketFile);
            LOG.debug("Adding " + bucketFile.toString() + " to files to read");
          } else {
            filesToRead.add(d);
            LOG.debug("Adding " + d + " to files to read");
          }
        }
        splits[bucket] = new CompactorInputSplit(entries, bucket, filesToRead, baseDir,
            deltaDirs.toArray(new Path[deltaDirs.size()]));
      }
      LOG.debug("Returning " + splits.length + " splits");
      return splits;
    }

    @Override
    public RecordReader<RecordIdentifier, V> getRecordReader(InputSplit inputSplit, JobConf entries,
                                                         Reporter reporter) throws IOException {
      CompactorInputSplit split = (CompactorInputSplit)inputSplit;
      AcidInputFormat<V> aif =
          instantiate(AcidInputFormat.class, entries.get(INPUT_FORMAT_CLASS_NAME));
      ValidTxnList txnList =
          new ValidTxnListImpl(entries.get(ValidTxnList.VALID_TXNS_KEY));
      return aif.getRawReader(entries, entries.getBoolean(IS_MAJOR, false), split.getBucket(),
          txnList, split.getBaseDir(), split.getDeltaDirs());
    }
  }

  static class CompactorMap<V extends Writable>
      implements Mapper<RecordIdentifier, V,  NullWritable,  NullWritable> {

    JobConf jobConf;
    FSRecordWriter writer;

    @Override
    public void map(RecordIdentifier identifier, V value,
                    OutputCollector<NullWritable, NullWritable> nullWritableVOutputCollector,
                    Reporter reporter) throws IOException {
      // After all this setup there's actually almost nothing to do.
      getWriter(reporter, identifier.getBucketId()); // Make sure we've opened the writer
      writer.write(value);
    }

    @Override
    public void close() throws IOException {
      if (writer != null) writer.close(false);
    }

    @Override
    public void configure(JobConf entries) {
      jobConf = entries;
    }

    private void getWriter(Reporter reporter, int bucket) throws IOException {
      if (writer == null) {
        AbstractSerDe serde = instantiate(AbstractSerDe.class, jobConf.get(SERDE));
        ObjectInspector inspector;
        try {
          inspector = serde.getObjectInspector();
        } catch (SerDeException e) {
          LOG.error("Unable to get object inspector, " + StringUtils.stringifyException(e));
          throw new IOException(e);
        }

        AcidOutputFormat.Options options = new AcidOutputFormat.Options(jobConf);
        options.inspector(inspector)
            .writingBase(jobConf.getBoolean(IS_MAJOR, false))
            .isCompressed(jobConf.getBoolean(IS_COMPRESSED, false))
            .tableProperties(new StringableMap(jobConf.get(TABLE_PROPS)).toProperties())
            .reporter(reporter)
            .minimumTransactionId(jobConf.getLong(MIN_TXN, Long.MAX_VALUE))
            .maximumTransactionId(jobConf.getLong(MAX_TXN, Long.MIN_VALUE))
            .bucket(bucket);

        // Instantiate the underlying output format
        AcidOutputFormat<V> aof =
            instantiate(AcidOutputFormat.class, jobConf.get(OUTPUT_FORMAT_CLASS_NAME));

        Path location = AcidUtils.createFilename(new Path(jobConf.get(LOCATION)), options);
        writer = aof.getRawRecordWriter(location, options);
      }
    }
  }

  static class StringableMap extends HashMap<String, String> {

    StringableMap(String s) {
      String[] parts = s.split(":", 2);
      // read that many chars
      int numElements = Integer.valueOf(parts[0]);
      s = parts[1];
      for (int i = 0; i < numElements; i++) {
        parts = s.split(":", 2);
        int len = Integer.valueOf(parts[0]);
        String key = null;
        if (len > 0) key = parts[1].substring(0, len);
        parts = parts[1].substring(len).split(":", 2);
        len = Integer.valueOf(parts[0]);
        String value = null;
        if (len > 0) value = parts[1].substring(0, len);
        s = parts[1].substring(len);
        put(key, value);
      }
    }

    StringableMap(Map<String, String> m) {
      super(m);
    }

    @Override
    public String toString() {
      StringBuffer buf = new StringBuffer();
      buf.append(size());
      buf.append(':');
      if (size() > 0) {
        for (Map.Entry<String, String> entry : entrySet()) {
          int length = (entry.getKey() == null) ? 0 : entry.getKey().length();
          buf.append(entry.getKey() == null ? 0 : length);
          buf.append(':');
          if (length > 0) buf.append(entry.getKey());
          length = (entry.getValue() == null) ? 0 : entry.getValue().length();
          buf.append(length);
          buf.append(':');
          if (length > 0) buf.append(entry.getValue());
        }
      }
      return buf.toString();
    }

    public Properties toProperties() {
      Properties props = new Properties();
      props.putAll(this);
      return props;
    }
  }

  static class StringableList extends ArrayList<Path> {
    StringableList() {

    }

    StringableList(String s) {
      String[] parts = s.split(":", 2);
      // read that many chars
      int numElements = Integer.valueOf(parts[0]);
      s = parts[1];
      for (int i = 0; i < numElements; i++) {
        parts = s.split(":", 2);
        int len = Integer.valueOf(parts[0]);
        String val = parts[1].substring(0, len);
        s = parts[1].substring(len);
        add(new Path(val));
      }
    }

    @Override
    public String toString() {
      StringBuffer buf = new StringBuffer();
      buf.append(size());
      buf.append(':');
      if (size() > 0) {
        for (Path p : this) {
          buf.append(p.toString().length());
          buf.append(':');
          buf.append(p.toString());
        }
      }
      return buf.toString();
    }
  }

  private static <T> T instantiate(Class<T> classType, String classname) throws IOException {
    T t = null;
    try {
      Class c = Class.forName(classname);
      Object o = c.newInstance();
      if (classType.isAssignableFrom(o.getClass())) {
        t = (T)o;
      } else {
        String s = classname + " is not an instance of " + classType.getName();
        LOG.error(s);
        throw new IOException(s);
      }
    } catch (ClassNotFoundException e) {
      LOG.error("Unable to instantiate class, " + StringUtils.stringifyException(e));
      throw new IOException(e);
    } catch (InstantiationException e) {
      LOG.error("Unable to instantiate class, " + StringUtils.stringifyException(e));
      throw new IOException(e);
    } catch (IllegalAccessException e) {
      LOG.error("Unable to instantiate class, " + StringUtils.stringifyException(e));
      throw new IOException(e);
    }
    return t;
  }
}
