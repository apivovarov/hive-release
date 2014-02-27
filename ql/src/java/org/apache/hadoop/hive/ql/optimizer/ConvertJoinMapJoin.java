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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.MuxOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.OptimizeTezProcContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.MetaInfo;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.Statistics;

/**
 * ConvertJoinMapJoin is an optimization that replaces a common join
 * (aka shuffle join) with a map join (aka broadcast or fragment replicate
 * join when possible. Map joins have restrictions on which joins can be
 * converted (e.g.: full outer joins cannot be handled as map joins) as well
 * as memory restrictions (one side of the join has to fit into memory).
 */
public class ConvertJoinMapJoin implements NodeProcessor {

  static final private Log LOG = LogFactory.getLog(ConvertJoinMapJoin.class.getName());

  @Override
  /*
   * (non-Javadoc)
   * we should ideally not modify the tree we traverse.
   * However, since we need to walk the tree at any time when we modify the
   * operator, we might as well do it here.
   */
  public Object process(Node nd, Stack<Node> stack,
      NodeProcessorCtx procCtx, Object... nodeOutputs)
      throws SemanticException {

    OptimizeTezProcContext context = (OptimizeTezProcContext) procCtx;

    if (!context.conf.getBoolVar(HiveConf.ConfVars.HIVECONVERTJOIN)) {
      return null;
    }

    JoinOperator joinOp = (JoinOperator) nd;
    int mapJoinConversionPos = mapJoinConversionPos(joinOp, context);
    if (mapJoinConversionPos < 0) {
      return null;
    }
    
    if (context.conf.getBoolVar(HiveConf.ConfVars.HIVE_CONVERT_JOIN_BUCKET_MAPJOIN_TEZ)) {
      if (convertJoinBucketMapJoin(joinOp, context, mapJoinConversionPos)) {
        return null;
      }
    }
    
    convertJoinMapJoin(joinOp, context, mapJoinConversionPos);
    return null;
  }
  
  private boolean convertJoinBucketMapJoin(JoinOperator joinOp, OptimizeTezProcContext context, 
      int bigTablePosition) throws SemanticException {
    
    TezBucketJoinProcCtx tezBucketJoinProcCtx = new TezBucketJoinProcCtx(context.conf);
    
    if (!checkConvertJoinBucketMapJoin(joinOp, context, bigTablePosition, tezBucketJoinProcCtx)) {
      return false;
    }
    
    createRoutingTable(tezBucketJoinProcCtx);

    MapJoinOperator mapJoinOp = 
        convertJoinMapJoin(joinOp, context, bigTablePosition);

    mapJoinOp.getConf().setBucketMapJoin(true);
    Map<String, Integer> bigTableBucketNumMapping = new HashMap<String, Integer>();
    bigTableBucketNumMapping.put(mapJoinOp.getConf().getBigTableAlias(), tezBucketJoinProcCtx.getNumBuckets());
    mapJoinOp.getConf().setBigTableBucketNumMapping(bigTableBucketNumMapping );

    // Once the conversion is done, we can set the partitioner to bucket cols on the small table    
    return true;
  }

  private void createRoutingTable(TezBucketJoinProcCtx tezBucketJoinProcCtx) throws SemanticException {
    Map<Partition, List<String>> partToBucketFileNames = tezBucketJoinProcCtx.getBigTblPartsToBucketFileNames();
    Map<Partition, Integer> partToBucketNumber = tezBucketJoinProcCtx.getBigTblPartsToBucketNumber();
    
    for (List<String> partBucketNames : partToBucketFileNames.values()) {
      Collections.sort(partBucketNames);
    }
    
    Iterator<Entry<Partition, List<String>>> partToBucketNames =
        partToBucketFileNames.entrySet().iterator();
    Iterator<Entry<Partition, Integer>> bigTblPartToBucketNum = partToBucketNumber
        .entrySet().iterator();
    Map<String, List<Integer>> routingTable = new HashMap<String, List<Integer>>();
    while (partToBucketNames.hasNext()) {
      assert bigTblPartToBucketNum.hasNext();
      int bigTblBucketNum = bigTblPartToBucketNum.next().getValue();
      List<String> bigTblBucketNameList = partToBucketNames.next().getValue();
      
      int i = 0;
      for (String bucketName : bigTblBucketNameList) {
        List<Integer> bucketNumList = routingTable.get(bucketName);  
        if (bucketNumList == null) {
          bucketNumList = new ArrayList<Integer>();
        }
        if (i > bigTblBucketNum) {
          throw new SemanticException("The number of buckets " + i + 
              " is greater than the computed number " + bigTblBucketNum);
        }
        bucketNumList.add(i++);
        routingTable.put(bucketName, bucketNumList);
      }
    }
    
    tezBucketJoinProcCtx.setRoutingTable(routingTable);
    return;
  }

  private boolean checkConvertJoinBucketMapJoin(JoinOperator joinOp, 
      OptimizeTezProcContext context, int bigTablePosition, 
      TezBucketJoinProcCtx tezBucketJoinProcCtx) throws SemanticException {
    // bail on mux-operator
    if (joinOp.getParentOperators().get(0) instanceof MuxOperator) {
      return false;
    }
    ReduceSinkOperator rs = (ReduceSinkOperator) joinOp.getParentOperators().get(bigTablePosition);
    Operator<? extends OperatorDesc> parentOfRS = rs.getParentOperators().get(0);
    int i = 0;
    if (parentOfRS.getMetaInfo().getKeyCols() != null) {
      tezBucketJoinProcCtx.setIsSubQuery(true);
      for (ExprNodeDesc exprNodeDesc : parentOfRS.getMetaInfo().getKeyCols()) {
        if (rs.getMetaInfo().getKeyCols().get(i).isSame(exprNodeDesc)) {
          i++;
        } else {
          return false;
        }
      }
      tezBucketJoinProcCtx.setNumBuckets(i);
    } else if (rs.getMetaInfo().getTable() != null) {
      Table table = rs.getMetaInfo().getTable();
      if (table.getBucketCols() == null || table.getBucketCols().isEmpty()) {
        return false;
      }
      tezBucketJoinProcCtx.setIsSubQuery(false);
      tezBucketJoinProcCtx.setNumBuckets(table.getBucketCols().size());
      // check if number of files per partition is equal to the number of buckets
      // check if bucket keys are equal and in the same order as join keys
      return AbstractBucketJoinProc.canConvertBucketMapJoin(table, context.parseContext, 
          tezBucketJoinProcCtx, rs.getMetaInfo().getPrunedPartList(), 
          rs.getMetaInfo().getKeyCols());
    }

    // no information available to check for bucket map join
    return false;
  }

  public int mapJoinConversionPos(JoinOperator joinOp, OptimizeTezProcContext context) {
    Set<Integer> bigTableCandidateSet = MapJoinProcessor.
      getBigTableCandidates(joinOp.getConf().getConds());

    long maxSize = context.conf.getLongVar(
      HiveConf.ConfVars.HIVECONVERTJOINNOCONDITIONALTASKTHRESHOLD);

    int bigTablePosition = -1;

    Statistics bigInputStat = null;
    long totalSize = 0;
    int pos = 0;

    // bigTableFound means we've encountered a table that's bigger than the
    // max. This table is either the the big table or we cannot convert.
    boolean bigTableFound = false;

    for (Operator<? extends OperatorDesc> parentOp : joinOp.getParentOperators()) {

      Statistics currInputStat = parentOp.getStatistics();
      if (currInputStat == null) {
        LOG.warn("Couldn't get statistics from: "+parentOp);
        return -1;
      }

      long inputSize = currInputStat.getDataSize();
      if ((bigInputStat == null) ||
          ((bigInputStat != null) &&
           (inputSize > bigInputStat.getDataSize()))) {

        if (bigTableFound) {
          // cannot convert to map join; we've already chosen a big table
          // on size and there's another one that's bigger.
          return -1;
        }

        if (inputSize > maxSize) {
          if (!bigTableCandidateSet.contains(pos)) {
            // can't use the current table as the big table, but it's too
            // big for the map side.
            return -1;
          }

          bigTableFound = true;
        }

        if (bigInputStat != null) {
          // we're replacing the current big table with a new one. Need
          // to count the current one as a map table then.
          totalSize += bigInputStat.getDataSize();
        }

        if (totalSize > maxSize) {
          // sum of small tables size in this join exceeds configured limit
          // hence cannot convert.
          return -1;
        }

        if (bigTableCandidateSet.contains(pos)) {
          bigTablePosition = pos;
          bigInputStat = currInputStat;
        }
      } else {
        totalSize += currInputStat.getDataSize();
        if (totalSize > maxSize) {
          // cannot hold all map tables in memory. Cannot convert.
          return -1;
        }
      }
      pos++;
    }

    return bigTablePosition;
  }

  /*
   * Once we have decided on the map join, the tree would transform from
   *
   *        |                   |
   *       Join               MapJoin
   *       / \                /   \
   *     RS   RS   --->     RS    TS (big table)
   *    /      \           /
   *   TS       TS        TS (small table)
   *
   * for tez.
   */

  public MapJoinOperator convertJoinMapJoin(JoinOperator joinOp, OptimizeTezProcContext context, 
      int bigTablePosition) throws SemanticException {
    // bail on mux operator
    for (Operator<? extends OperatorDesc> parentOp : joinOp.getParentOperators()) {
      if (parentOp instanceof MuxOperator) {
        return null;
      }
    }
    
    //can safely convert the join to a map join.
    ParseContext parseContext = context.parseContext;
    MapJoinOperator mapJoinOp = MapJoinProcessor.
      convertJoinOpMapJoinOp(context.conf, parseContext.getOpParseCtx(),
      joinOp, parseContext.getJoinContext().get(joinOp), bigTablePosition, true);

    Operator<? extends OperatorDesc> parentBigTableOp
      = mapJoinOp.getParentOperators().get(bigTablePosition);

    if (parentBigTableOp instanceof ReduceSinkOperator) {
      mapJoinOp.getParentOperators().remove(bigTablePosition);
      if (!(mapJoinOp.getParentOperators().contains(
          parentBigTableOp.getParentOperators().get(0)))) {
        mapJoinOp.getParentOperators().add(bigTablePosition,
          parentBigTableOp.getParentOperators().get(0));
      }
      parentBigTableOp.getParentOperators().get(0).removeChild(parentBigTableOp);
      for (Operator<? extends OperatorDesc> op : mapJoinOp.getParentOperators()) {
        if (!(op.getChildOperators().contains(mapJoinOp))) {
          op.getChildOperators().add(mapJoinOp);
        }
        op.getChildOperators().remove(joinOp);
      }
    }
    
    mapJoinOp.setMetaInfo(new MetaInfo(null, null, null));
 
    return mapJoinOp;
  }
}
