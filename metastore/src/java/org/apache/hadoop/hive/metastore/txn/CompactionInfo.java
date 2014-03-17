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
package org.apache.hadoop.hive.metastore.txn;

import org.apache.hadoop.hive.metastore.api.CompactionType;

/**
 * Information on a possible or running compaction.
 */
public class CompactionInfo {
  public long id;
  public String dbname;
  public String tableName;
  public String partName;
  public CompactionType type;
  public String runAs;
  public boolean tooManyAborts = false;

  private String fullPartitionName = null;
  private String fullTableName = null;

  public String getFullPartitionName() {
    if (fullPartitionName == null) {
      StringBuilder buf = new StringBuilder(dbname);
      buf.append('.');
      buf.append(tableName);
      if (partName != null) {
        buf.append('.');
        buf.append(partName);
      }
      fullPartitionName = buf.toString();
    }
    return fullPartitionName;
  }

  public String getFullTableName() {
    if (fullTableName == null) {
      StringBuilder buf = new StringBuilder(dbname);
      buf.append('.');
      buf.append(tableName);
      fullTableName = buf.toString();
    }
    return fullTableName;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof CompactionInfo)) return false;
    CompactionInfo other = (CompactionInfo)o;
    if (!dbname.equals(other.dbname)) return false;
    if (!tableName.equals(other.tableName)) return false;
    if (partName != null) return partName.equals(other.partName);
    else return other.partName == null;
  }

  @Override
  public int hashCode() {
    int hash = dbname.hashCode() ^ tableName.hashCode();
    if (partName != null) hash ^= partName.hashCode();
    return hash;
  }
}
