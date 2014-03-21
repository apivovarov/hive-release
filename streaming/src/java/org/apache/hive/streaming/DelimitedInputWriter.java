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


import com.google.common.annotations.VisibleForTesting;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

/**
 * Streaming Writer handles delimited input (eg. CSV).
 * Delimited input is parsed & reordered to match column order in table
 *
 */
public class DelimitedInputWriter extends AbstractLazySimpleRecordWriter {
  private final boolean reorderingNeeded;
  private String delimiter;
  private int[] fieldToColMapping;

 /**
  * @param colNamesForFields Column name assignment for input fields. nulls or empty
  *                          strings in the array indicates the fields to be skipped
  * @param delimiter input field delimiter
  * @param endPoint Hive endpoint
  * @throws ConnectionError Problem talking to Hive
  * @throws ClassNotFoundException Serde class not found
  * @throws SerializationError Serde initialization/interaction failed
  * @throws StreamingException Problem acquiring file system path for partition
  * @throws InvalidColumn any element in colNamesForFields refers to a non existing column
  */
   public DelimitedInputWriter(String[] colNamesForFields, String delimiter,
                              HiveEndPoint endPoint)
          throws ClassNotFoundException, ConnectionError, SerializationError,
                 InvalidColumn, StreamingException {
    super(endPoint);
    this.delimiter = delimiter;
    this.fieldToColMapping = getFieldReordering(colNamesForFields, getTableColumns());
    this.reorderingNeeded = isReorderingNeeded(delimiter, getTableColumns());
  }

  /**
   *
   * @param colNamesForFields Column name assignment for input fields
   * @param delimiter input field delimiter
   * @param endPoint Hive endpoint
   * @param outputSerdeSeparator separator used for the LazySimpleSerde
   * @throws ConnectionError Problem talking to Hive
   * @throws ClassNotFoundException Serde class not found
   * @throws SerializationError Serde initialization/interaction failed
   * @throws StreamingException Problem acquiring file system path for partition
   * @throws InvalidColumn any element in colNamesForFields refers to a non existing column
   */
  public DelimitedInputWriter(String[] colNamesForFields, String delimiter,
                              HiveEndPoint endPoint, char outputSerdeSeparator)
          throws ClassNotFoundException, ConnectionError, SerializationError,
                 InvalidColumn, StreamingException {
    super(endPoint, outputSerdeSeparator);
    this.delimiter = delimiter;
    this.fieldToColMapping = getFieldReordering(colNamesForFields, getTableColumns());
    this.reorderingNeeded = isReorderingNeeded(delimiter, getTableColumns());
  }

  private boolean isReorderingNeeded(String delimiter, ArrayList<String> tableColumns) {
    return !( delimiter.equals(String.valueOf(getSerdeSeparator()))
            && areFieldsInColOrder(fieldToColMapping)
            && tableColumns.size()>=fieldToColMapping.length );
  }

  private static boolean areFieldsInColOrder(int[] fieldToColMapping) {
    for(int i=0; i<fieldToColMapping.length; ++i) {
      if(fieldToColMapping[i]!=i) {
        return false;
      }
    }
    return true;
  }

  @VisibleForTesting
  static int[] getFieldReordering(String[] colNamesForFields, List<String> tableColNames)
          throws InvalidColumn {
    int[] result = new int[ colNamesForFields.length ];
    for(int i=0; i<colNamesForFields.length; ++i) {
      result[i] = -1;
    }
    int i=-1, fieldLabelCount=0;
    for( String col : colNamesForFields ) {
      ++i;
      if(col == null) {
        continue;
      }
      ++fieldLabelCount;
      if( col.trim().isEmpty() ) {
        continue;
      }
      int loc = tableColNames.indexOf(col);
      if(loc == -1) {
        throw new InvalidColumn("Column '" + col + "' not found in table for input field " + i+1);
      }
      result[i] = loc;
    }
    if(fieldLabelCount>tableColNames.size()) {
      throw new InvalidColumn("Number of field names exceeds the number of columns in table");
    }
    return result;
  }

  // Reorder fields in record based on the order of columns in the table
  protected byte[] reorderFields(byte[] record) throws UnsupportedEncodingException {
    if(!reorderingNeeded) {
      return record;
    }
    String[] reorderedFields = new String[getTableColumns().size()];
    String decoded = new String(record);
    String[] fields = decoded.split(delimiter);
    for (int i=0; i<fieldToColMapping.length; ++i) {
      int newIndex = fieldToColMapping[i];
      if(newIndex != -1) {
        reorderedFields[newIndex] = fields[i];
      }
    }
    return join(reorderedFields,getSerdeSeparator());
  }

  // handles nulls in items[]
  // TODO: perhaps can be made more efficient by creating a byte[] directly
  private static byte[] join(String[] items, char separator) {
    StringBuffer buff = new StringBuffer(100);
    if(items.length == 0)
      return "".getBytes();
    int i=0;
    for(; i<items.length-1; ++i) {
      if(items[i]!=null) {
        buff.append(items[i]);
      }
      buff.append(separator);
    }
    if(items[i]!=null) {
      buff.append(items[i]);
    }
    return buff.toString().getBytes();
  }

}
