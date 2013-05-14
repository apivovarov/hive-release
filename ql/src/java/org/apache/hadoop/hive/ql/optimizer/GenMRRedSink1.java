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

package org.apache.hadoop.hive.ql.optimizer;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.optimizer.GenMRProcContext.GenMapRedCtx;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

/**
 * Processor for the rule - table scan followed by reduce sink.
 */
public class GenMRRedSink1 implements NodeProcessor {

  public GenMRRedSink1() {
  }

  private boolean checkMultiparentOps(Operator <? extends OperatorDesc> op) {
    while (op != null) {
      if (op.getParentOperators() == null) {
        break;
      }
      if (op.getParentOperators().size() > 1) {
        return true;
      } else {
        op = op.getParentOperators().get(0);
      }
    }

    return false;
  }

  private boolean areMultiAncestorsMultiWayOps (ReduceSinkOperator reduceOp) {
    Operator<? extends OperatorDesc> childOp = reduceOp.getChildOperators().get(0);
    List<Operator<? extends OperatorDesc>> parentOps = childOp.getParentOperators();
    if (parentOps.size() > 1) {
      int count = 0;
      for (Operator<? extends OperatorDesc> op : parentOps) {
        if (checkMultiparentOps(op)) {
          count++;
        }
        if (count > 1) {
          return true;
        }
      }
    }

    return false;
  }

  /**
   * Reduce Sink encountered.
   * a) If we are seeing this RS for first time, we initialize plan corresponding to this RS.
   * b) If we are seeing this RS for second or later time then either query had a join in which
   *    case we will merge this plan with earlier plan involving this RS or plan for this RS
   *    needs to be split in two branches.
   *
   * @param nd
   *          the reduce sink operator encountered
   * @param opProcCtx
   *          context
   */
  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx opProcCtx,
      Object... nodeOutputs) throws SemanticException {
    ReduceSinkOperator op = (ReduceSinkOperator) nd;
    GenMRProcContext ctx = (GenMRProcContext) opProcCtx;

    boolean multiWayOp = areMultiAncestorsMultiWayOps(op);

    Map<Operator<? extends OperatorDesc>, GenMapRedCtx> mapCurrCtx = ctx
        .getMapCurrCtx();
    GenMapRedCtx mapredCtx = mapCurrCtx.get(stack.get(stack.size() - 2));
    Task<? extends Serializable> currTask = mapredCtx.getCurrTask();
    MapredWork currPlan = (MapredWork) currTask.getWork();
    Operator<? extends OperatorDesc> currTopOp = mapredCtx.getCurrTopOp();
    String currAliasId = mapredCtx.getCurrAliasId();
    Operator<? extends OperatorDesc> reducer = op.getChildOperators().get(0);
    HashMap<Operator<? extends OperatorDesc>, Task<? extends Serializable>> opTaskMap = ctx
        .getOpTaskMap();
    Task<? extends Serializable> opMapTask = opTaskMap.get(reducer);

    ctx.setCurrTopOp(currTopOp);
    ctx.setCurrAliasId(currAliasId);
    ctx.setCurrTask(currTask);

    // If the plan for this reducer does not exist, initialize the plan
    if (opMapTask == null) {
      if (currPlan.getReducer() == null) {
        if (!multiWayOp) {
          GenMapRedUtils.initPlan(op, ctx);
        } else {
          GenMapRedUtils.splitPlan(op, ctx);
          List<Operator<? extends OperatorDesc>> splitTaskOps =
              ctx.getSplitTasks();
          splitTaskOps.add(reducer);
        }
      } else {
        GenMapRedUtils.splitPlan(op, ctx);
      }
    } else {
      // This will happen in case of joins. The current plan can be thrown away
      // after being merged with the original plan or if we split the plan, we
      // need to ensure that we correctly hook up the corresponding reducer to 
      // this other task.
      Task<? extends Serializable> oldTask = null;
      if (ctx.getSplitTasks().contains(reducer)) {
        multiWayOp = true;
      }
      if (multiWayOp) {
        oldTask = ctx.getCurrTask();
      }
      GenMapRedUtils.joinPlan(op, oldTask, opMapTask, ctx, -1, multiWayOp);
      currTask = opMapTask;
      ctx.setCurrTask(currTask);
    }

    mapCurrCtx.put(op, new GenMapRedCtx(ctx.getCurrTask(), ctx.getCurrTopOp(),
        ctx.getCurrAliasId()));
    return null;
  }

}
