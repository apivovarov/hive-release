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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hcatalog.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * WrapperRecordWriterContainer wraps a HiveRecordWriter and implements a mapred.RecordWriter
 * so that a RecordWriterContainer can contain it.
 *
 */

class WrapperRecordWriter implements RecordWriter {

    org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter hrw = null;

    public WrapperRecordWriter(
        org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter hiveRecordWriter) {
        this.hrw = hiveRecordWriter;
    }

    @Override
    public void close(Reporter arg0) throws IOException {
        // no reporter gets passed in the HiveRecordWriter,
        // only an abort param, which we default to false
        hrw.close(false);
    }

    @Override
    public void write(Object key, Object value) throws IOException {
        // key is ignored, value is the writable record to be written
        hrw.write((Writable) value);
    }

}
