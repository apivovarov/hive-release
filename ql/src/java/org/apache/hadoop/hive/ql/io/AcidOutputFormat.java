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

package org.apache.hadoop.hive.ql.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Properties;

public interface AcidOutputFormat<V> extends HiveOutputFormat<NullWritable, V> {

  public static class Options {
    private final Configuration configuration;
    private FileSystem fs;
    private ObjectInspector inspector;
    private boolean writingBase = false;
    private boolean isCompressed = false;
    private Properties properties;
    private Reporter reporter;
    private long minimumTransactionId;
    private long maximumTransactionId;
    private int bucket;
    private PrintStream dummyStream = null;
    private boolean oldStyle = false;

    public Options(Configuration conf) {
      this.configuration = conf;
    }

    public Options inspector(ObjectInspector inspector) {
      this.inspector = inspector;
      return this;
    }

    public Options writingBase(boolean val) {
      this.writingBase = val;
      return this;
    }

    public Options filesystem(FileSystem fs) {
      this.fs = fs;
      return this;
    }

    public Options isCompressed(boolean isCompressed) {
      this.isCompressed = isCompressed;
      return this;
    }

    public Options tableProperties(Properties properties) {
      this.properties = properties;
      return this;
    }

    public Options reporter(Reporter reporter) {
      this.reporter = reporter;
      return this;
    }

    public Options minimumTransactionId(long min) {
      this.minimumTransactionId = min;
      return this;
    }

    public Options maximumTransactionId(long max) {
      this.maximumTransactionId = max;
      return this;
    }

    public Options bucket(int bucket) {
      this.bucket = bucket;
      return this;
    }

    Options setOldStyle(boolean value) {
      oldStyle = value;
      return this;
    }

    /**
     * Temporary switch while we are in development that replaces the
     * implementation with a dummy one that just prints to stream.
     * @param stream the stream to print to
     * @return this
     */
    public Options useDummy(PrintStream stream) {
      this.dummyStream = stream;
      return this;
    }

    public Configuration getConfiguration() {
      return configuration;
    }

    public FileSystem getFilesystem() {
      return fs;
    }

    public ObjectInspector getInspector() {
      return inspector;
    }

    public boolean isCompressed() {
      return isCompressed;
    }

    public Properties getTableProperties() {
      return properties;
    }

    public Reporter getReporter() {
      return reporter;
    }

    public long getMinimumTransactionId() {
      return minimumTransactionId;
    }

    public long getMaximumTransactionId() {
      return maximumTransactionId;
    }

    public boolean isWritingBase() {
      return writingBase;
    }

    public int getBucket() {
      return bucket;
    }

    public PrintStream getDummyStream() {
      return dummyStream;
    }

    boolean getOldStyle() {
      return oldStyle;
    }
  }

  /**
   * Create a RecordUpdater for inserting, updating, or deleting records.
   * @param path the partition directory name
   * @param options the options for the writer
   * @return the RecordUpdater for the output file
   */
  public RecordUpdater getRecordUpdater(Path path,
                                        Options options) throws IOException;

  /**
   * Create a raw writer for ACID events.
   * This is only intended for the compactor.
   * @param path the root directory
   * @param options options for writing the file
   * @return a record writer
   * @throws IOException
   */
  public FSRecordWriter getRawRecordWriter(Path path,
                                           Options options) throws IOException;
}
