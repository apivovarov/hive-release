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
import org.apache.hadoop.yarn.api.records.apptimeline.ATSEntity;
import org.apache.hadoop.yarn.api.records.apptimeline.ATSEvent;
import org.apache.hadoop.yarn.client.api.TimelineClient;

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
  private enum OtherInfoTypes { query };
  private enum PrimaryFilterTypes { user };

  public ATSHook() {
    executor = Executors.newSingleThreadExecutor(
         new ThreadFactoryBuilder().setDaemon(true).setNameFormat("ATS Logger %d").build());
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
              fireAndForget(hookContext.getConf(), createPostHookEvent(queryId, currentTime, user));
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

  ATSEntity createPreHookEvent(String queryId, String query, JSONObject explainPlan, 
      long startTime, String user) throws Exception {

    JSONObject otherInfo = new JSONObject();
    JSONObject queryObj = new JSONObject();
    queryObj.put("queryText", query);
    queryObj.put("queryPlan", explainPlan);
    otherInfo.put("query", queryObj);
  
    LOG.warn("Otherinfo: "+otherInfo.toString());
    
    ATSEntity atsEntity = new ATSEntity();
    atsEntity.setEntityId(queryId);
    atsEntity.setEntityType(EntityTypes.HIVE_QUERY_ID.name());
    atsEntity.addPrimaryFilter(PrimaryFilterTypes.user.name(), user);
    
    ATSEvent startEvt = new ATSEvent();
    startEvt.setEventType(EventTypes.QUERY_SUBMITTED.name());
    startEvt.setTimestamp(startTime);
    atsEntity.addEvent(startEvt);

    atsEntity.addOtherInfo(OtherInfoTypes.query.name(), otherInfo.toString());
    return atsEntity;
  }

  ATSEntity createPostHookEvent(String queryId, long stopTime, String user) {

    ATSEntity atsEntity = new ATSEntity();
    atsEntity.setEntityId(queryId);
    atsEntity.setEntityType(EntityTypes.HIVE_QUERY_ID.name());
    atsEntity.addPrimaryFilter(PrimaryFilterTypes.user.name(), user);
    
    ATSEvent stopEvt = new ATSEvent();
    stopEvt.setEventType(EventTypes.QUERY_COMPLETED.name());
    stopEvt.setTimestamp(stopTime);
    atsEntity.addEvent(stopEvt);
    
    return atsEntity;
  }
  
  void fireAndForget(Configuration conf, ATSEntity entity) throws Exception {
    TimelineClient timelineClient = TimelineClient.createTimelineClient();
    timelineClient.init(conf);
    timelineClient.start();
    timelineClient.postEntities(entity);
    timelineClient.stop();
  }
}
