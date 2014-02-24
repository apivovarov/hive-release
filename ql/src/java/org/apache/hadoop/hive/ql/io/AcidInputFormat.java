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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient.ValidTxnList;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public interface AcidInputFormat<V>
    extends InputFormat<NullWritable, V>, InputFormatChecker {

  public static class Options {
    private final Configuration conf;
    private Reporter reporter;

    public Options(Configuration conf) {
      this.conf = conf;
    }

    public Options reporter(Reporter reporter) {
      this.reporter = reporter;
      return this;
    }

    public Configuration getConfiguration() {
      return conf;
    }

    public Reporter getReporter() {
      return reporter;
    }
  }

  public static interface RowReader<V>
      extends RecordReader<RecordIdentifier, V> {
    public ObjectInspector getObjectInspector();
  }

  public RowReader<V> getReader(InputSplit split,
                                Options options) throws IOException;

  public static interface RawReader<V>
      extends RecordReader<RecordIdentifier, V> {
    public ObjectInspector getObjectInspector();
  }

  /**
   * Get a reader that returns the raw ACID events (insert, update, delete).
   * Should only be used by the compactor.
   * @param conf the configuration
   * @param collapseEvents should the ACID events be collapsed so that only
   *                       the last version of the row is kept.
   * @param bucket the bucket to read
   * @param validTxnList the list of valid transactions to use
   * @param baseDirectory the base directory to read or the root directory for
   *                      old style files
   * @param deltaDirectory a list of delta files to include in the merge
   * @return a record reader
   * @throws IOException
   */
   RawReader<V> getRawReader(Configuration conf,
                             boolean collapseEvents,
                             int bucket,
                             ValidTxnList validTxnList,
                             Path baseDirectory,
                             Path[] deltaDirectory
                             ) throws IOException;
}
