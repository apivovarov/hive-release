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

package org.apache.hadoop.hive.ql.optimizer.metainfo.annotation;

import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.OpTraits;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

public class OpTraitsRulesProcFactory {

  public static class DefaultRule implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      @SuppressWarnings("unchecked")
      Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>)nd;
      op.setOpTraits(op.getParentOperators().get(0).getOpTraits());
      return null;
    }

  }

  /*
   * Reduce sink operator is the de-facto operator 
   * for determining keyCols (emit keys of a map phase)
   */
  public static class ReduceSinkRule implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      
      ReduceSinkOperator rs = (ReduceSinkOperator)nd;
      OpTraits opTraits = new OpTraits(rs.getConf().getKeyCols(), 
          rs.getParentOperators().get(0).getOpTraits().getTable(), 
          rs.getParentOperators().get(0).getOpTraits().getPrunedPartList());
      rs.setOpTraits(opTraits);
      return null;
    }
  }

  /*
   * Table scan has the table object and pruned partitions that has information such as 
   * bucketing, sorting, etc. that is used later for optimization.
   */
  public static class TableScanRule implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      TableScanOperator ts = (TableScanOperator)nd;
      AnnotateOpTraitsProcCtx opTraitsCtx = (AnnotateOpTraitsProcCtx)procCtx;
      Table table = opTraitsCtx.getParseContext().getTopToTable().get(ts);
      PrunedPartitionList prunedPartList = null;
      try {
        prunedPartList = 
            opTraitsCtx.getParseContext().getPrunedPartitions(ts.getConf().getAlias(), ts);
      } catch (HiveException e) {
        prunedPartList = null;
      }
      OpTraits opTraits = new OpTraits(null, table, prunedPartList);
      ts.setOpTraits(opTraits);
      return null;
    }
  }

  /*
   * Group-by re-orders the keys emitted hence, the keyCols would change.
   */
  public static class GroupByRule implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      GroupByOperator gbyOp = (GroupByOperator)nd;
      OpTraits opTraits = new OpTraits(gbyOp.getConf().getKeys(), 
          gbyOp.getParentOperators().get(0).getOpTraits().getTable(), 
          gbyOp.getParentOperators().get(0).getOpTraits().getPrunedPartList());
      gbyOp.setOpTraits(opTraits);
      return null;
    }
  }
  
  /*
   *  When we have operators that have multiple parents, it is not
   *  clear which parent's traits we need to propagate forward. 
   */
  public static class MultiParentRule implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      OpTraits opTraits = new OpTraits(null, null, null);
      @SuppressWarnings("unchecked")
      Operator<? extends OperatorDesc> operator = (Operator<? extends OperatorDesc>)nd;
      operator.setOpTraits(opTraits);
      return null;
    } 
  }

  public static NodeProcessor getTableScanRule() {
    return new TableScanRule();
  }

  public static NodeProcessor getReduceSinkRule() {
    return new ReduceSinkRule();
  }

  public static NodeProcessor getDefaultRule() {
    return new DefaultRule();
  }

  public static NodeProcessor getMultiParentRule() {
    return new MultiParentRule();
  }

  public static NodeProcessor getGroupByRule() {
    return new GroupByRule();
  }

}
