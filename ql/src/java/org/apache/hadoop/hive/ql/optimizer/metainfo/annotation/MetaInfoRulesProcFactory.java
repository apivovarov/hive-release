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
import org.apache.hadoop.hive.ql.plan.MetaInfo;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

public class MetaInfoRulesProcFactory {

  public static class DefaultRule implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      @SuppressWarnings("unchecked")
      Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>)nd;
      op.setMetaInfo(op.getParentOperators().get(0).getMetaInfo());
      return null;
    }

  }

  public static class ReduceSinkRule implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      
      ReduceSinkOperator rs = (ReduceSinkOperator)nd;
      MetaInfo metaInfo = new MetaInfo(rs.getConf().getKeyCols(), 
          rs.getParentOperators().get(0).getMetaInfo().getTable(), 
          rs.getParentOperators().get(0).getMetaInfo().getPrunedPartList());
      rs.setMetaInfo(metaInfo);
      return null;
    }
  }

  public static class TableScanRule implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      TableScanOperator ts = (TableScanOperator)nd;
      AnnotateMetaInfoProcCtx metaInfoCtx = (AnnotateMetaInfoProcCtx)procCtx;
      Table table = metaInfoCtx.getParseContext().getTopToTable().get(ts);
      PrunedPartitionList prunedPartList = null;
      try {
        prunedPartList = 
            metaInfoCtx.getParseContext().getPrunedPartitions(ts.getConf().getAlias(), ts);
      } catch (HiveException e) {
        prunedPartList = null;
      }
      MetaInfo metaInfo = new MetaInfo(null, table, prunedPartList);
      ts.setMetaInfo(metaInfo);
      return null;
    }
  }
  
  public static class GroupByRule implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      GroupByOperator gbyOp = (GroupByOperator)nd;
      MetaInfo metaInfo = new MetaInfo(gbyOp.getConf().getKeys(), 
          gbyOp.getParentOperators().get(0).getMetaInfo().getTable(), 
          gbyOp.getParentOperators().get(0).getMetaInfo().getPrunedPartList());
      gbyOp.setMetaInfo(metaInfo);
      return null;
    }
  }
  
  public static class MultiParentRule implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      MetaInfo metaInfo = new MetaInfo(null, null, null);
      @SuppressWarnings("unchecked")
      Operator<? extends OperatorDesc> operator = (Operator<? extends OperatorDesc>)nd;
      operator.setMetaInfo(metaInfo);
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
