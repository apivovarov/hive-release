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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * A class to clean directories after compactions.  This will run in a separate thread.
 */
public class Cleaner extends CompactorThread {
  static final private String CLASS_NAME = Cleaner.class.getName();
  static final private Log LOG = LogFactory.getLog(CLASS_NAME);

  private long cleanerCheckInterval = 5000;

  @Override
  public void run() {
    // Make sure nothing escapes this run method and kills the metastore at large,
    // so wrap it in a big catch Throwable statement.
    while (!stop.timeToStop) {
      try {
        long startedAt = System.currentTimeMillis();

        // Now look for new entries ready to be cleaned.
        List<CompactionInfo> toClean = txnHandler.findReadyToClean();
        for (CompactionInfo ci : toClean) {
          LockComponent comp = null;
          comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.TABLE, ci.dbname);
          comp.setTablename(ci.tableName);
          if (ci.partName != null)  comp.setPartitionname(ci.partName);
          List<LockComponent> components = new ArrayList<LockComponent>(1);
          components.add(comp);
          LockRequest rqst = new LockRequest(components, System.getProperty("user.name"),
              Worker.hostname());
          LockResponse rsp = txnHandler.lockNoWait(rqst);
          try {
            if (rsp.getState() == LockState.ACQUIRED) {
              clean(ci);
            }
          } finally {
            txnHandler.unlock(new UnlockRequest(rsp.getLockid()));
          }
        }

        // Now, go back to bed until it's time to do this again
        long elapsedTime = System.currentTimeMillis() - startedAt;
        if (elapsedTime >= cleanerCheckInterval)  continue;
        else Thread.sleep(cleanerCheckInterval - elapsedTime);
      } catch (Throwable t) {
        LOG.error("Caught an exception in the main loop of compactor cleaner, " +
            StringUtils.stringifyException(t));
      }
    }
  }

  private void clean(CompactionInfo ci) {
    String s = "Starting cleaning for " + ci.getFullPartitionName();
    LOG.info(s);
    try {
      StorageDescriptor sd = resolveStorageDescriptor(resolveTable(ci), resolvePartition(ci));
      String location = sd.getLocation();

      // Create a bogus validTxnList with a high water mark set to MAX_LONG and no open
      // transactions.  This assures that all deltas are treated as valid and all we return are
      // obsolete files.
      GetOpenTxnsResponse rsp = new GetOpenTxnsResponse(Long.MAX_VALUE, new HashSet<Long>());
      IMetaStoreClient.ValidTxnList txnList = new HiveMetaStoreClient.ValidTxnListImpl(rsp);

      AcidUtils.Directory dir = AcidUtils.getAcidState(new Path(location), conf, txnList);
      List<FileStatus> obsoleteDirs = dir.getObsolete();
      final List<Path> filesToDelete = new ArrayList<Path>(obsoleteDirs.size());
      for (FileStatus stat : obsoleteDirs) {
        filesToDelete.add(stat.getPath());
      }
      if (filesToDelete.size() < 1) {
        LOG.warn("Hmm, nothing to delete in the cleaner for directory " + location +
            ", that hardly seems right.");
        return;
      }
      final FileSystem fs = filesToDelete.get(0).getFileSystem(conf);

      if (runJobAsSelf(ci.runAs)) {
        removeFiles(fs, filesToDelete);
      } else {
        UserGroupInformation ugi = UserGroupInformation.createProxyUser(ci.runAs,
          UserGroupInformation.getLoginUser());
        ugi.doAs(new PrivilegedExceptionAction<Object>() {
          @Override
          public Object run() throws Exception {
            removeFiles(fs, filesToDelete);
            return null;
          }
        });
      }

    } catch (Exception e) {
      LOG.error("Caught exception when cleaning, unable to complete cleaning " +
          StringUtils.stringifyException(e));
    } finally {
      // We need to clean this out one way or another.
      txnHandler.markCleaned(ci);
    }

  }

  private void removeFiles(FileSystem fs, List<Path> deadFilesWalking) throws IOException {
    for (Path dead : deadFilesWalking) {
      LOG.debug("Doing to delete path " + dead.toString());
      fs.delete(dead, true);
    }

  }

}
