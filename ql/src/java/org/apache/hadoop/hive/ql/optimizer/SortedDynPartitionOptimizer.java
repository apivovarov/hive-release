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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.ExtractOperator;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.DynamicPartitionCtx;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExtractDesc;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * When dynamic partitioning (with or without bucketing and sorting) is enabled,
 * this optimization sorts the records on partition, bucket and sort columns
 * respectively before inserting records into the destination table. This enables
 * reducers to keep only one record writer all the time thereby reducing the
 * the memory pressure on the reducers. This optimization will force a reducer
 * even when hive.enforce.bucketing and hive.enforce.sorting is set to false.
 *
 */
public class SortedDynPartitionOptimizer implements Transform {

  @Override
  public ParseContext transform(ParseContext pCtx) throws SemanticException {

    // create a walker which walks the tree in a DFS manner while maintaining the
    // operator stack. The dispatcher generates the plan from the operator tree
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();

    String TS = TableScanOperator.getOperatorName() + "%";
    String RS = ReduceSinkOperator.getOperatorName() + "%";
    String FS = FileSinkOperator.getOperatorName() + "%";
    String FIL = FilterOperator.getOperatorName() + "%";
    String SEL = SelectOperator.getOperatorName() + "%";
    String EX = ExtractOperator.getOperatorName() + "%";

    // MR plan
    opRules.put(new RuleRegExp("R3", RS + EX + FS), getMapReduceProc(pCtx));

    // Map only plan
    opRules.put(new RuleRegExp("R1", TS + FIL + SEL + FS), getMapOnlyProc(pCtx));
    opRules.put(new RuleRegExp("R2", TS + SEL + FS), getMapOnlyProc(pCtx));

    Dispatcher disp = new DefaultRuleDispatcher(null, opRules, null);
    GraphWalker ogw = new DefaultGraphWalker(disp);

    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pCtx.getTopOps().values());
    ogw.startWalking(topNodes, null);

    return pCtx;
  }

  private NodeProcessor getMapOnlyProc(ParseContext pCtx) {
    return new SortedDynPartMapOnlyProc(pCtx);
  }

  private NodeProcessor getMapReduceProc(ParseContext pCtx) {
    return new SortedDynPartMRProc(pCtx);
  }

  class SortedDynPartMapOnlyProc implements NodeProcessor {

    protected ParseContext parseCtx;

    public SortedDynPartMapOnlyProc(ParseContext pCtx) {
      this.parseCtx = pCtx;
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      // If the reduce sink has not been introduced due to bucketing/sorting, ignore it
      FileSinkOperator fsOp = (FileSinkOperator) nd;

      // skip if parent is not SEL
      if (!(fsOp.getParentOperators().get(0) instanceof SelectOperator)) {
        return null;
      }

      // if not dynamic partitioning then bail out
      if (fsOp.getConf().getDynPartCtx() == null) {
        return null;
      }

      SelectOperator selOp = (SelectOperator) fsOp.getParentOperators().get(0);
      DynamicPartitionCtx dpCtx = fsOp.getConf().getDynPartCtx();
      Table destTable = parseCtx.getFsopToTable().get(fsOp);
      if (destTable == null) {
        return null;
      }

      // Get the positions for partition, bucket and sort columns in case of any
      List<Integer> bucketPositions = getBucketPositions(destTable.getBucketCols(),
          destTable.getCols());
      ObjectPair<List<Integer>, List<Integer>> sortOrderPositions = getSortPositionsOrder(
          destTable.getSortCols(), destTable.getCols());
      List<Integer> sortPositions = sortOrderPositions.getFirst();
      List<Integer> sortOrder = sortOrderPositions.getSecond();
      List<Integer> partitionPositions = getPartitionPositionsColNames(dpCtx, selOp.getConf()
          .getOutputColumnNames());
      int numBuckets = destTable.getNumBuckets();
      ArrayList<ExprNodeDesc> bucketColumns = getBucketPositionsToExprNodes(bucketPositions, selOp
          .getConf().getColList());

      // update file sink descriptor
      fsOp.getConf().setBucketCols(bucketColumns);
      fsOp.getConf().setMultiFileSpray(false);
      fsOp.getConf().setNumFiles(1);
      fsOp.getConf().setTotalFiles(1);

      // Insert RS and EX between SEL and FS. New order will become
      // SEL -> RS -> EX -> FS.

      // Clear off SEL children
      selOp.getChildOperators().clear();

      // Prepare ReduceSinkDesc
      RowResolver inputRR = parseCtx.getOpParseCtx().get(selOp).getRowResolver();
      ObjectPair<String, RowResolver> pair = copyRowResolver(inputRR);
      RowResolver outRR = pair.getSecond();
      ArrayList<ColumnInfo> valColInfo = Lists.newArrayList(selOp.getSchema().getSignature());
      ArrayList<ExprNodeDesc> newValueCols = Lists.newArrayList();
      Map<String, ExprNodeDesc> colExprMap = Maps.newHashMap();
      for (ColumnInfo ci : valColInfo) {
        newValueCols.add(new ExprNodeColumnDesc(ci.getType(), ci.getInternalName(), ci
            .getTabAlias(), ci.isHiddenVirtualCol()));
        colExprMap.put(ci.getInternalName(), newValueCols.get(newValueCols.size() - 1));
      }
      ReduceSinkDesc rsConf = getReduceSinkDesc(partitionPositions, bucketPositions, sortPositions,
          sortOrder, newValueCols, bucketColumns, numBuckets, selOp);

      // Create ReduceSink operator
      ReduceSinkOperator rsOp = (ReduceSinkOperator) putOpInsertMap(
          OperatorFactory.getAndMakeChild(rsConf, new RowSchema(outRR.getColumnInfos()), selOp),
          outRR, parseCtx);
      rsOp.setColumnExprMap(colExprMap);

      // Create ExtractDesc
      ObjectPair<String, RowResolver> exPair = copyRowResolver(outRR);
      RowResolver exRR = exPair.getSecond();
      ExtractDesc exConf = new ExtractDesc(new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo,
          Utilities.ReduceField.VALUE.toString(), "", false));

      // Create Extract Operator
      ExtractOperator exOp = (ExtractOperator) putOpInsertMap(
          OperatorFactory.getAndMakeChild(exConf, new RowSchema(exRR.getColumnInfos()), rsOp),
          exRR, parseCtx);

      // Attach the newly created operators to FileSink
      FileSinkOperator newFsOp = null;
      try {
        RowResolver fsRR = parseCtx.getOpParseCtx().get(fsOp).getRowResolver();
        Table table = parseCtx.getFsopToTable().get(fsOp);
        newFsOp = (FileSinkOperator) fsOp.cloneOp();
        parseCtx.removeOpParseCtx(fsOp);
        parseCtx.getFsopToTable().remove(fsOp);
        parseCtx.getFsopToTable().put(newFsOp, table);
        parseCtx.getOpParseCtx().put(newFsOp, new OpParseContext(fsRR));
        newFsOp.getParentOperators().add(exOp);
      } catch (CloneNotSupportedException e) {
        // do not proceed with this optimization
        e.printStackTrace();
        return null;
      }
      exOp.getChildOperators().add(newFsOp);

      // Set if partition sorted or partition bucket sorted
      newFsOp.getConf().setDpSortState(FileSinkDesc.DPSortState.PARTITION_SORTED);
      if (bucketColumns.size() > 1) {
        newFsOp.getConf().setDpSortState(FileSinkDesc.DPSortState.PARTITION_BUCKET_SORTED);
        updateRowResolvers(parseCtx, rsOp);
        updateReduceSinkKeyTable(rsOp);
        updateReduceSinkValueTable(rsOp);
      }
      return null;
    }

  }

  class SortedDynPartMRProc implements NodeProcessor {

    protected ParseContext parseCtx;

    public SortedDynPartMRProc(ParseContext pCtx) {
      this.parseCtx = pCtx;
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      FileSinkOperator fsOp = (FileSinkOperator) nd;
      if (!(fsOp.getParentOperators().get(0) instanceof ExtractOperator)) {
        return null;
      }

      // if not dynamic partitioning then bail out
      if (fsOp.getConf().getDynPartCtx() == null) {
        return null;
      }

      ExtractOperator exOp = (ExtractOperator) fsOp.getParentOperators().get(0);
      if (!(exOp.getParentOperators().get(0) instanceof ReduceSinkOperator)) {
        return null;
      }
      ReduceSinkOperator rsOp = (ReduceSinkOperator) exOp.getParentOperators().get(0);
      DynamicPartitionCtx dpCtx = fsOp.getConf().getDynPartCtx();
      HashMap<String, Operator<? extends OperatorDesc>> tops = parseCtx.getTopOps();

      // We do not deal with multiple TS ops at this point
      if (tops.size() > 1) {
        return null;
      }

      TableScanOperator tsOp = null;
      for (Map.Entry<String, Operator<? extends OperatorDesc>> entry : tops.entrySet()) {
        tsOp = (TableScanOperator) entry.getValue();
      }

      Table srcTable = parseCtx.getTopToTable().get(tsOp);
      Table destTable = parseCtx.getFsopToTable().get(fsOp);
      if (destTable == null || srcTable == null) {
        return null;
      }

      // Get the positions for partition, bucket and sort columns
      List<Integer> bucketPositions = getBucketPositions(destTable.getBucketCols(),
          destTable.getCols());
      ObjectPair<List<Integer>, List<Integer>> sortOrderPositions = getSortPositionsOrder(
          destTable.getSortCols(), destTable.getCols());
      List<Integer> sortPositions = sortOrderPositions.getFirst();
      List<Integer> sortOrder = sortOrderPositions.getSecond();
      List<Integer> partitionPositions = getPartitionPositions(dpCtx, rsOp.getConf().getValueCols());
      int numBuckets = destTable.getNumBuckets();
      ArrayList<ExprNodeDesc> bucketColumns = getBucketPositionsToExprNodes(bucketPositions, rsOp
          .getConf().getValueCols());

      // update file sink descriptor
      fsOp.getConf().setBucketCols(bucketColumns);
      fsOp.getConf().setMultiFileSpray(false);
      fsOp.getConf().setNumFiles(1);
      fsOp.getConf().setTotalFiles(1);

      // update reduce sink descriptors
      rsOp.getConf().setBucketCols(bucketColumns);
      rsOp.getConf().setNumBuckets(numBuckets);
      updateReduceSinkKey(partitionPositions, bucketPositions, sortPositions, sortOrder, rsOp);
      if (bucketColumns.size() > 0) {
        fsOp.getConf().setDpSortState(FileSinkDesc.DPSortState.PARTITION_BUCKET_SORTED);
        updateRowResolvers(parseCtx, rsOp);
        updateReduceSinkKeyTable(rsOp);
        updateReduceSinkValueTable(rsOp);
      }
      return null;
    }
  }

  // Get the bucket positions for the table
  private List<Integer> getBucketPositions(List<String> tabBucketCols, List<FieldSchema> tabCols) {
    List<Integer> posns = new ArrayList<Integer>();
    for (String bucketCol : tabBucketCols) {
      int pos = 0;
      for (FieldSchema tabCol : tabCols) {
        if (bucketCol.equals(tabCol.getName())) {
          posns.add(pos);
          break;
        }
        pos++;
      }
    }
    return posns;
  }

  public ReduceSinkDesc getReduceSinkDesc(List<Integer> partitionPositions,
      List<Integer> bucketPositions, List<Integer> sortPositions, List<Integer> sortOrder,
      ArrayList<ExprNodeDesc> newValueCols, ArrayList<ExprNodeDesc> bucketColumns, int numBuckets,
      SelectOperator selOp) {

    // Order of KEY columns
    // 1) Partition columns
    // 2) Bucket columns
    // 3) Sort columns
    List<Integer> keyColsPosInVal = Lists.newArrayList();
    ArrayList<ExprNodeDesc> newKeyCols = Lists.newArrayList();
    List<Integer> newSortOrder = Lists.newArrayList();

    keyColsPosInVal.addAll(partitionPositions);
    keyColsPosInVal.addAll(bucketPositions);
    keyColsPosInVal.addAll(sortPositions);

    // by default partition and bucket columns are sorted in ascending order
    Integer order = null;
    if (sortOrder != null && !sortOrder.isEmpty()) {
      if (sortOrder.get(0).intValue() == 1) {
        order = 1;
      } else {
        order = 0;
      }
    }
    for (int i = 0; i < (partitionPositions.size() + bucketPositions.size()); i++) {
      newSortOrder.add(order);
    }
    newSortOrder.addAll(sortOrder);

    List<Integer> partColsPosInVal = Lists.newArrayList();
    ArrayList<ExprNodeDesc> newPartCols = Lists.newArrayList();

    // Order of DISTRIBUTION columns
    // 1) Partition columns
    // 2) Bucket columns
    partColsPosInVal.addAll(partitionPositions);
    partColsPosInVal.addAll(bucketPositions);

    // we will clone here as RS will update bucket column key with its
    // corresponding with bucker number and hence their OIs
    for (Integer idx : keyColsPosInVal) {
      newKeyCols.add(newValueCols.get(idx).clone());
    }

    for (Integer idx : partColsPosInVal) {
      newPartCols.add(newValueCols.get(idx).clone());
    }

    String orderStr = "";
    for (int i = 0; i < newKeyCols.size(); i++) {
      orderStr += "+";
    }

    // Create Key/Value TableDesc. When the operator plan is split into MR tasks,
    // the reduce operator will initialize Extract operator with information
    // from Key and Value TableDesc
    List<FieldSchema> fields = PlanUtils.getFieldSchemasFromColumnList(newKeyCols, "reducesinkkey");
    TableDesc keyTable = PlanUtils.getReduceKeyTableDesc(fields, orderStr);
    ArrayList<String> outputKeyCols = Lists.newArrayList();
    for (int i = 0; i < newKeyCols.size(); i++) {
      outputKeyCols.add("reducesinkkey" + i);
    }

    ArrayList<String> outValColNames = Lists.newArrayList(selOp.getConf().getOutputColumnNames());
    List<FieldSchema> valFields = PlanUtils.getFieldSchemasFromColumnList(newValueCols,
        outValColNames, 0, "");
    TableDesc valueTable = PlanUtils.getReduceValueTableDesc(valFields);

    List<List<Integer>> distinctColumnIndices = Lists.newArrayList();
    ReduceSinkDesc rsConf = new ReduceSinkDesc(newKeyCols, newKeyCols.size(), newValueCols,
        outputKeyCols, distinctColumnIndices, outValColNames, -1, newPartCols, -1, keyTable,
        valueTable);
    rsConf.setBucketCols(bucketColumns);
    rsConf.setNumBuckets(numBuckets);

    return rsConf;
  }

  /**
   * Returns the position of partitions columns given their ExprNodeDesc
   * @param dpCtx
   * @param cols
   * @return
   */
  private List<Integer> getPartitionPositions(DynamicPartitionCtx dpCtx,
      ArrayList<ExprNodeDesc> cols) {
    List<Integer> posns = Lists.newArrayList();
    List<String> internalCols = Lists.newArrayList();
    for (String dpCol : dpCtx.getDPColNames()) {
      for (Map.Entry<String, String> entry : dpCtx.getInputToDPCols().entrySet()) {
        if (entry.getValue().equalsIgnoreCase(dpCol)) {
          internalCols.add(entry.getKey());
        }
      }
    }

    for (String partCol : internalCols) {
      int pos = 0;
      for (ExprNodeDesc tabCol : cols) {
        String colName = ((ExprNodeColumnDesc) tabCol).getColumn();
        if (partCol.equals(colName)) {
          posns.add(pos);
          break;
        }
        pos++;
      }
    }
    return posns;
  }

  /**
   * Returns the position of partition columns given their Column Names
   * @param dpCtx
   * @param colNames
   * @return
   */
  private List<Integer> getPartitionPositionsColNames(DynamicPartitionCtx dpCtx,
      List<String> colNames) {
    List<Integer> posns = Lists.newArrayList();
    List<String> internalCols = Lists.newArrayList();
    for (String dpCol : dpCtx.getDPColNames()) {
      for (Map.Entry<String, String> entry : dpCtx.getInputToDPCols().entrySet()) {
        if (entry.getValue().equalsIgnoreCase(dpCol)) {
          internalCols.add(entry.getKey());
        }
      }
    }

    for (String partCol : internalCols) {
      int pos = 0;
      for (String tabCol : colNames) {
        if (partCol.equals(tabCol)) {
          posns.add(pos);
          break;
        }
        pos++;
      }
    }
    return posns;
  }

  /**
   * Get the sort positions and sort order for the sort columns
   * @param tabSortCols
   * @param tabCols
   * @return
   */
  private ObjectPair<List<Integer>, List<Integer>> getSortPositionsOrder(List<Order> tabSortCols,
      List<FieldSchema> tabCols) {
    List<Integer> sortPositions = Lists.newArrayList();
    List<Integer> sortOrders = Lists.newArrayList();
    for (Order sortCol : tabSortCols) {
      int pos = 0;
      for (FieldSchema tabCol : tabCols) {
        if (sortCol.getCol().equals(tabCol.getName())) {
          sortPositions.add(pos);
          sortOrders.add(sortCol.getOrder());
          break;
        }
        pos++;
      }
    }
    return new ObjectPair<List<Integer>, List<Integer>>(sortPositions, sortOrders);
  }

  /**
   * Refreshes the Key TableDesc based on updates to ReduceSinkDesc
   * @param rsOp
   */
  private void updateReduceSinkKeyTable(ReduceSinkOperator rsOp) {
    ArrayList<ExprNodeDesc> keyCols = rsOp.getConf().getKeyCols();
    String order = rsOp.getConf().getOrder();
    String newOrder = "";
    if (order == null || order.isEmpty() || order.startsWith("+")) {
      order = "+";
    } else {
      order = "-";
    }

    for (int i = 0; i < keyCols.size(); i++) {
      newOrder += order;
    }
    List<FieldSchema> fields = PlanUtils.getFieldSchemasFromColumnList(keyCols, "reducesinkkey");
    TableDesc keyTable = PlanUtils.getReduceKeyTableDesc(fields, newOrder);
    ArrayList<String> outputKeyCols = Lists.newArrayList();
    for (int i = 0; i < keyCols.size(); i++) {
      outputKeyCols.add("reducesinkkey" + i);
    }
    rsOp.getConf().setKeySerializeInfo(keyTable);
    rsOp.getConf().setOutputKeyColumnNames(outputKeyCols);
  }

  /**
   * Refreshes the Value TableDesc based on updates to ReduceSinkDesc
   * @param rsOp
   */
  private void updateReduceSinkValueTable(ReduceSinkOperator rsOp) {
    ArrayList<ExprNodeDesc> valueCols = rsOp.getConf().getValueCols();
    ArrayList<String> outValColNames = rsOp.getConf().getOutputValueColumnNames();
    List<FieldSchema> fields = PlanUtils.getFieldSchemasFromColumnList(valueCols, outValColNames,
        0, "");
    TableDesc valueTable = PlanUtils.getReduceValueTableDesc(fields);
    rsOp.getConf().setValueSerializeInfo(valueTable);
  }

  private ArrayList<ExprNodeDesc> getBucketPositionsToExprNodes(List<Integer> bucketPositions,
      List<ExprNodeDesc> valueCols) {
    ArrayList<ExprNodeDesc> bucketCols = Lists.newArrayList();

    for (Integer idx : bucketPositions) {
      bucketCols.add(valueCols.get(idx));
    }

    return bucketCols;
  }

  private void updateReduceSinkKey(List<Integer> partitionPositions, List<Integer> bucketPositions,
      List<Integer> sortPositions, List<Integer> sortOrder, ReduceSinkOperator rsOp) {

    List<ExprNodeDesc> valueCols = rsOp.getConf().getValueCols();

    // Order of KEY columns
    // 1) Partition columns
    // 2) Bucket columns
    // 3) Sort columns
    List<Integer> keyColsPosInVal = Lists.newArrayList();
    ArrayList<ExprNodeDesc> newKeyCols = Lists.newArrayList();
    List<Integer> newSortOrder = Lists.newArrayList();

    keyColsPosInVal.addAll(partitionPositions);
    keyColsPosInVal.addAll(bucketPositions);
    keyColsPosInVal.addAll(sortPositions);

    // by default partition and bucket columns are sorted in ascending order
    Integer order = null;
    if (sortOrder != null && !sortOrder.isEmpty()) {
      if (sortOrder.get(0).intValue() == 1) {
        order = 1;
      } else {
        order = 0;
      }
    }
    for (int i = 0; i < (partitionPositions.size() + bucketPositions.size()); i++) {
      newSortOrder.add(order);
    }
    newSortOrder.addAll(sortOrder);

    List<Integer> partColsPosInVal = Lists.newArrayList();
    ArrayList<ExprNodeDesc> newPartCols = Lists.newArrayList();

    // Order of DISTRIBUTION columns
    // 1) Partition columns
    // 2) Bucket columns
    partColsPosInVal.addAll(partitionPositions);
    partColsPosInVal.addAll(bucketPositions);

    // we will clone here as RS will update bucket column with bucker number
    for (Integer idx : keyColsPosInVal) {
      newKeyCols.add(valueCols.get(idx).clone());
    }

    for (Integer idx : partColsPosInVal) {
      newPartCols.add(valueCols.get(idx).clone());
    }

    rsOp.getConf().setKeyCols(newKeyCols);
    rsOp.getConf().setNumDistributionKeys(newKeyCols.size());
    rsOp.getConf().setPartitionCols(newPartCols);
  }

  /**
   * Updated RowResolvers of RS and all of its children
   * @param pCtx
   * @param rsOp
   */
  private void updateRowResolvers(ParseContext pCtx, ReduceSinkOperator rsOp) {
    ArrayList<ExprNodeDesc> valueCols = rsOp.getConf().getValueCols();
    Map<String, ExprNodeDesc> colExprMap = rsOp.getColumnExprMap();
    RowResolver inputRR = pCtx.getOpParseCtx().get(rsOp).getRowResolver();
    ArrayList<ColumnInfo> outColInfos = inputRR.getColumnInfos();
    ObjectPair<String, RowResolver> tabToRR = copyRowResolver(inputRR);
    String tabAlias = tabToRR.getFirst();
    RowResolver outRR = tabToRR.getSecond();

    // insert new hidden column _bcol0 to value columns
    ColumnInfo buckCol = new ColumnInfo("_bcol0", TypeInfoFactory.intTypeInfo, tabAlias, true);
    ExprNodeColumnDesc encd = new ExprNodeColumnDesc(buckCol.getType(), buckCol.getInternalName(),
        buckCol.getAlias(), buckCol.getIsVirtualCol());
    valueCols.add(encd);
    colExprMap.put(buckCol.getInternalName(), valueCols.get(valueCols.size() - 1));
    outColInfos.add(buckCol);
    outRR.put(tabAlias, "_bcol0", buckCol);
    rsOp.getConf().getOutputValueColumnNames().add("_bcol0");
    rsOp.setSchema(outRR.getRowSchema());
    pCtx.getOpParseCtx().get(rsOp).setRowResolver(outRR);

    // update the row resolver of children
    for (Operator<? extends OperatorDesc> child : rsOp.getChildOperators()) {
      updateChildRowResolvers(pCtx, outRR, child);
    }
  }

  private void updateChildRowResolvers(ParseContext pCtx, RowResolver inputRR,
      Operator<? extends OperatorDesc> op) {
    if (op == null) {
      return;
    }

    ObjectPair<String, RowResolver> tabToRR = copyRowResolver(inputRR);
    RowResolver outRR = tabToRR.getSecond();

    op.setSchema(outRR.getRowSchema());
    pCtx.getOpParseCtx().get(op).setRowResolver(outRR);

    for (Operator<? extends OperatorDesc> child : op.getChildOperators()) {
      updateChildRowResolvers(pCtx, outRR, child);
    }
  }

  private Operator<? extends Serializable> putOpInsertMap(Operator<?> op, RowResolver rr,
      ParseContext context) {
    OpParseContext ctx = new OpParseContext(rr);
    context.getOpParseCtx().put(op, ctx);
    return op;
  }

  private ObjectPair<String, RowResolver> copyRowResolver(RowResolver inputRR) {
    ObjectPair<String, RowResolver> output = new ObjectPair<String, RowResolver>();
    RowResolver outRR = new RowResolver();
    int pos = 0;
    String tabAlias = null;

    for (ColumnInfo colInfo : inputRR.getColumnInfos()) {
      String[] info = inputRR.reverseLookup(colInfo.getInternalName());
      tabAlias = info[0];
      outRR.put(info[0], info[1], new ColumnInfo(SemanticAnalyzer.getColumnInternalName(pos),
          colInfo.getType(), info[0], colInfo.getIsVirtualCol(), colInfo.isHiddenVirtualCol()));
      pos++;
    }
    output.setFirst(tabAlias);
    output.setSecond(outRR);
    return output;
  }
}
