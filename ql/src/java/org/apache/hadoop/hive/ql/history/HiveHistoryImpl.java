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

package org.apache.hadoop.hive.ql.history;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.Counters.Group;

/**
 * HiveHistory. Logs information such as query, query plan, runtime statistics
 * for into a file.
 * Each session uses a new object, which creates a new file.
 */
public class HiveHistoryImpl implements HiveHistory{

  PrintWriter histStream; // History File stream

  String histFileName; // History file name

  private static final Log LOG = LogFactory.getLog("hive.ql.exec.HiveHistory");

  private LogHelper console;

  private Map<String, String> idToTableMap = null;

  // Job Hash Map
  private final HashMap<String, QueryInfo> queryInfoMap = new HashMap<String, QueryInfo>();

  // Task Hash Map
  private final HashMap<String, TaskInfo> taskInfoMap = new HashMap<String, TaskInfo>();

  private static final String DELIMITER = " ";

  private static final String ROW_COUNT_PATTERN = "TABLE_ID_(\\d+)_ROWCOUNT";

  private static final Pattern rowCountPattern = Pattern.compile(ROW_COUNT_PATTERN);

  /**
   * Construct HiveHistory object an open history log file.
   *
   * @param ss
   */
  public HiveHistoryImpl(SessionState ss) {

    try {
      console = new LogHelper(LOG);
      String conf_file_loc = ss.getConf().getVar(
          HiveConf.ConfVars.HIVEHISTORYFILELOC);
      if ((conf_file_loc == null) || conf_file_loc.length() == 0) {
        console.printError("No history file location given");
        return;
      }

      // Create directory
      File f = new File(conf_file_loc);
      if (!f.exists()) {
        if (!f.mkdirs()) {
          console.printError("Unable to create log directory " + conf_file_loc);
          return;
        }
      }
      Random randGen = new Random();
      do {
        histFileName = conf_file_loc + "/hive_job_log_" + ss.getSessionId() + "_"
          + Math.abs(randGen.nextInt()) + ".txt";
      } while (new File(histFileName).exists());
      console.printInfo("Hive history file=" + histFileName);
      histStream = new PrintWriter(histFileName);

      HashMap<String, String> hm = new HashMap<String, String>();
      hm.put(Keys.SESSION_ID.name(), ss.getSessionId());
      log(RecordTypes.SessionStart, hm);
    } catch (FileNotFoundException e) {
      console.printError("FAILED: Failed to open Query Log : " + histFileName
          + " " + e.getMessage(), "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
    }

  }

  /**
   * @return historyFileName
   */
  public String getHistFileName() {
    return histFileName;
  }

  /**
   * Write the a history record to history file.
   *
   * @param rt
   * @param keyValMap
   */
  void log(RecordTypes rt, Map<String, String> keyValMap) {

    if (histStream == null) {
      return;
    }

    StringBuilder sb = new StringBuilder();
    sb.append(rt.name());

    for (Map.Entry<String, String> ent : keyValMap.entrySet()) {

      sb.append(DELIMITER);
      String key = ent.getKey();
      String val = ent.getValue();
      if(val != null) {
        val = val.replace('\n', ' ');
      }
      sb.append(key + "=\"" + val + "\"");

    }
    sb.append(DELIMITER);
    sb.append(Keys.TIME.name() + "=\"" + System.currentTimeMillis() + "\"");
    histStream.println(sb);
    histStream.flush();

  }

  /**
   * Called at the start of job Driver.execute().
   */
  public void startQuery(String cmd, String id) {
    SessionState ss = SessionState.get();
    if (ss == null) {
      return;
    }
    QueryInfo ji = new QueryInfo();

    ji.hm.put(Keys.QUERY_ID.name(), id);
    ji.hm.put(Keys.QUERY_STRING.name(), cmd);

    queryInfoMap.put(id, ji);

    log(RecordTypes.QueryStart, ji.hm);

  }

  /**
   * Used to set job status and other attributes of a job.
   *
   * @param queryId
   * @param propName
   * @param propValue
   */
  @Override
  public void setQueryProperty(String queryId, Keys propName, String propValue) {
    QueryInfo ji = queryInfoMap.get(queryId);
    if (ji == null) {
      return;
    }
    ji.hm.put(propName.name(), propValue);
  }

  /**
   * Used to set task properties.
   *
   * @param taskId
   * @param propName
   * @param propValue
   */
  @Override
  public void setTaskProperty(String queryId, String taskId, Keys propName,
      String propValue) {
    String id = queryId + ":" + taskId;
    TaskInfo ti = taskInfoMap.get(id);
    if (ti == null) {
      return;
    }
    ti.hm.put(propName.name(), propValue);
  }

  /**
   * Serialize the task counters and set as a task property.
   *
   * @param queryId
   * @param taskId
   * @param ctrs
   */
  public void setTaskCounters(String queryId, String taskId, Counters ctrs) {
    String id = queryId + ":" + taskId;
    QueryInfo ji = queryInfoMap.get(queryId);
    StringBuilder sb1 = new StringBuilder("");
    TaskInfo ti = taskInfoMap.get(id);
    if ((ti == null) || (ctrs == null)) {
      return;
    }
    StringBuilder sb = new StringBuilder("");
    try {

      boolean first = true;
      for (Group group : ctrs) {
        for (Counter counter : group) {
          if (first) {
            first = false;
          } else {
            sb.append(',');
          }
          sb.append(group.getDisplayName());
          sb.append('.');
          sb.append(counter.getDisplayName());
          sb.append(':');
          sb.append(counter.getCounter());
          String tab = getRowCountTableName(counter.getDisplayName());
          if (tab != null) {
            if (sb1.length() > 0) {
              sb1.append(",");
            }
            sb1.append(tab);
            sb1.append('~');
            sb1.append(counter.getCounter());
            ji.rowCountMap.put(tab, counter.getCounter());

          }
        }
      }

    } catch (Exception e) {
      LOG.warn(org.apache.hadoop.util.StringUtils.stringifyException(e));
    }
    if (sb1.length() > 0) {
      taskInfoMap.get(id).hm.put(Keys.ROWS_INSERTED.name(), sb1.toString());
      queryInfoMap.get(queryId).hm.put(Keys.ROWS_INSERTED.name(), sb1
          .toString());
    }
    if (sb.length() > 0) {
      taskInfoMap.get(id).hm.put(Keys.TASK_COUNTERS.name(), sb.toString());
    }
  }

  public void printRowCount(String queryId) {
    QueryInfo ji = queryInfoMap.get(queryId);
    if (ji == null) {
      return;
    }
    for (String tab : ji.rowCountMap.keySet()) {
      console.printInfo(ji.rowCountMap.get(tab) + " Rows loaded to " + tab);
    }
  }

  /**
   * Called at the end of Job. A Job is sql query.
   *
   * @param queryId
   */
  public void endQuery(String queryId) {
    QueryInfo ji = queryInfoMap.get(queryId);
    if (ji == null) {
      return;
    }
    log(RecordTypes.QueryEnd, ji.hm);
    queryInfoMap.remove(queryId);
  }

  /**
   * Called at the start of a task. Called by Driver.run() A Job can have
   * multiple tasks. Tasks will have multiple operator.
   *
   * @param task
   */
  public void startTask(String queryId, Task<? extends Serializable> task,
      String taskName) {
    SessionState ss = SessionState.get();
    if (ss == null) {
      return;
    }
    TaskInfo ti = new TaskInfo();

    ti.hm.put(Keys.QUERY_ID.name(), ss.getQueryId());
    ti.hm.put(Keys.TASK_ID.name(), task.getId());
    ti.hm.put(Keys.TASK_NAME.name(), taskName);

    String id = queryId + ":" + task.getId();
    taskInfoMap.put(id, ti);

    log(RecordTypes.TaskStart, ti.hm);

  }

  /**
   * Called at the end of a task.
   *
   * @param task
   */
  public void endTask(String queryId, Task<? extends Serializable> task) {
    String id = queryId + ":" + task.getId();
    TaskInfo ti = taskInfoMap.get(id);

    if (ti == null) {
      return;
    }
    log(RecordTypes.TaskEnd, ti.hm);
    taskInfoMap.remove(id);
  }

  /**
   * Called at the end of a task.
   *
   * @param task
   */
  public void progressTask(String queryId, Task<? extends Serializable> task) {
    String id = queryId + ":" + task.getId();
    TaskInfo ti = taskInfoMap.get(id);
    if (ti == null) {
      return;
    }
    log(RecordTypes.TaskProgress, ti.hm);

  }

  /**
   * write out counters.
   */
  static ThreadLocal<Map<String,String>> ctrMapFactory =
      new ThreadLocal<Map<String, String>>() {
        @Override
        protected Map<String,String> initialValue() {
          return new HashMap<String,String>();
        }
      };

  public void logPlanProgress(QueryPlan plan) throws IOException {
    Map<String,String> ctrmap = ctrMapFactory.get();
    ctrmap.put("plan", plan.toString());
    log(RecordTypes.Counters, ctrmap);
  }

  /**
   * Set the table to id map.
   *
   * @param map
   */
  public void setIdToTableMap(Map<String, String> map) {
    idToTableMap = map;
  }

  /**
   * Returns table name for the counter name.
   *
   * @param name
   * @return tableName
   */
  String getRowCountTableName(String name) {
    if (idToTableMap == null) {
      return null;
    }
    Matcher m = rowCountPattern.matcher(name);

    if (m.find()) {
      String tuple = m.group(1);
      return idToTableMap.get(tuple);
    }
    return null;

  }

  public void closeStream() {
    IOUtils.cleanup(LOG, histStream);
  }

  @Override
  public void finalize() throws Throwable {
    closeStream();
    super.finalize();
  }



}
