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

package org.apache.hadoop.hive.ql.hooks;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.ExplainTask;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import org.json.JSONObject;

import static org.apache.hadoop.hive.ql.hooks.HookContext.HookType.*;

/**
 * ATSHook sends query + plan info to Ambari Timeline Server
 */
public class ATSHook implements ExecuteWithHookContext {

  private static final Log LOG = LogFactory.getLog(ATSHook.class.getName());
  private final ExecutorService executor;
  private enum EntityTypes { HIVE_QUERY_ID };
  private enum EventTypes { QUERY_SUBMITTED, QUERY_COMPLETED };
  private enum OtherInfoTypes { query, status };
  private enum PrimaryFilterTypes { user };
  private YarnConfiguration yarnConf;

  public ATSHook() {
    executor = Executors.newSingleThreadExecutor(
         new ThreadFactoryBuilder().setDaemon(true).setNameFormat("ATS Logger %d").build());
    yarnConf = new YarnConfiguration();
  }

  @Override
  public void run(final HookContext hookContext) throws Exception {
    final long currentTime = System.currentTimeMillis();
    executor.submit(new Runnable() {
        @Override
        public void run() {
          try {
            QueryPlan plan = hookContext.getQueryPlan();
            String queryId = plan.getQueryId();
            long queryStartTime = plan.getQueryStartTime();
            String user = hookContext.getUgi().getUserName();

            switch(hookContext.getHookType()) {
            case PRE_EXEC_HOOK:
              ExplainTask explain = new ExplainTask();
              explain.initialize(hookContext.getConf(), plan, null);
              String query = plan.getQueryStr();
              JSONObject explainPlan = explain.getJSONPlan(null, null, plan.getRootTasks(),
                   plan.getFetchTask(), true, false, false);
              fireAndForget(hookContext.getConf(), createPreHookEvent(queryId, query,
                   explainPlan, queryStartTime, user));
              break;
            case POST_EXEC_HOOK:
              fireAndForget(hookContext.getConf(), createPostHookEvent(queryId, currentTime, user, true));
              break;
            case ON_FAILURE_HOOK:
              fireAndForget(hookContext.getConf(), createPostHookEvent(queryId, currentTime, user, false));
              break;
            default:
              //ignore
              break;
            }
          } catch (Exception e) {
            LOG.warn("Failed to submit plan to ATS: "+StringUtils.stringifyException(e));
          }
        }
      });
  }

  TimelineEntity createPreHookEvent(String queryId, String query, JSONObject explainPlan, 
      long startTime, String user) throws Exception {

    JSONObject queryObj = new JSONObject();
    queryObj.put("queryText", query);
    queryObj.put("queryPlan", explainPlan);
  
    if (LOG.isDebugEnabled()) {
      LOG.warn("Otherinfo: "+queryObj.toString());
    }

    TimelineEntity atsEntity = new TimelineEntity();
    atsEntity.setEntityId(queryId);
    atsEntity.setEntityType(EntityTypes.HIVE_QUERY_ID.name());
    atsEntity.addPrimaryFilter(PrimaryFilterTypes.user.name(), user);
    
    TimelineEvent startEvt = new TimelineEvent();
    startEvt.setEventType(EventTypes.QUERY_SUBMITTED.name());
    startEvt.setTimestamp(startTime);
    atsEntity.addEvent(startEvt);

    atsEntity.addOtherInfo(OtherInfoTypes.query.name(), queryObj.toString());
    return atsEntity;
  }

  TimelineEntity createPostHookEvent(String queryId, long stopTime, String user, boolean success) {

    TimelineEntity atsEntity = new TimelineEntity();
    atsEntity.setEntityId(queryId);
    atsEntity.setEntityType(EntityTypes.HIVE_QUERY_ID.name());
    atsEntity.addPrimaryFilter(PrimaryFilterTypes.user.name(), user);
    
    TimelineEvent stopEvt = new TimelineEvent();
    stopEvt.setEventType(EventTypes.QUERY_COMPLETED.name());
    stopEvt.setTimestamp(stopTime);
    atsEntity.addEvent(stopEvt);
    
    atsEntity.addOtherInfo(OtherInfoTypes.status.name(), success);

    return atsEntity;
  }
  
  void fireAndForget(Configuration conf, TimelineEntity entity) throws Exception {
    TimelineClient timelineClient = TimelineClient.createTimelineClient();
    timelineClient.init(yarnConf);
    timelineClient.start();
    timelineClient.putEntities(entity);
    timelineClient.stop();
  }
}
