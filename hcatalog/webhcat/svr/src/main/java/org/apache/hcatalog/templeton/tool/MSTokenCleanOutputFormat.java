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
package org.apache.hcatalog.templeton.tool;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hcatalog.common.HCatUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Outputformat that cancels hcatalog delegation token once job is done.
 */
public class MSTokenCleanOutputFormat
    extends NullOutputFormat<NullWritable, NullWritable> {
    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) {
        return new CleanupOutputCommitter();
    }

    static class CleanupOutputCommitter extends OutputCommitter {
        private static final Logger LOG
            = LoggerFactory.getLogger(CleanupOutputCommitter.class);

        @Override
        public void abortTask(TaskAttemptContext taskContext) { }

        @Override
        public void cleanupJob(JobContext jobContext) { }

        @Override
        public void commitTask(TaskAttemptContext taskContext) { }

        @Override
        public boolean needsTaskCommit(TaskAttemptContext taskContext) {
            return false;
        }

        @Override
        public void setupJob(JobContext jobContext) { }

        @Override
        public void setupTask(TaskAttemptContext taskContext) { }

        @Override
        public void commitJob(JobContext jobContext) {
            cancelHcatDelegationTokens(jobContext);
        }

        @Override
        public void abortJob(JobContext jobContext, JobStatus.State state) {
            cancelHcatDelegationTokens(jobContext);
        }

        private void cancelHcatDelegationTokens(JobContext context) {
            try{
                docancelHcatDelegationTokens(context);
            }catch(Throwable t){
                LOG.warn("Error cancelling delegation token", t);
            }
        }

        private void docancelHcatDelegationTokens(JobContext context) {
            //Cancel hive metastore token
            HiveMetaStoreClient client = null;
            try {
                HiveConf hiveConf = HCatUtil.getHiveConf(context.getConfiguration());
                client = HCatUtil.getHiveClient(hiveConf);
                String tokenStrForm = client.getTokenStrForm();
                if (tokenStrForm != null) {
                    client.cancelDelegationToken(tokenStrForm);
                }
            } catch (Exception e) {
                LOG.warn("Failed to cancel delegation token", e);
            } finally {
                HCatUtil.closeHiveClientQuietly(client);
            }
        }


    };

}
