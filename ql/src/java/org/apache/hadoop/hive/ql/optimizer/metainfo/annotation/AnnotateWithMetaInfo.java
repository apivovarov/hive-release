package org.apache.hadoop.hive.ql.optimizer.metainfo.annotation;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.DemuxOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.MuxOperator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.PreOrderWalker;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.optimizer.Transform;
import org.apache.hadoop.hive.ql.optimizer.metainfo.annotation.MetaInfoRulesProcFactory;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class AnnotateWithMetaInfo implements Transform {

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    AnnotateMetaInfoProcCtx annotateCtx = new AnnotateMetaInfoProcCtx(pctx);

    // create a walker which walks the tree in a DFS manner while maintaining the
    // operator stack. The dispatcher generates the plan from the operator tree
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("TS", TableScanOperator.getOperatorName() + "%"),
        MetaInfoRulesProcFactory.getTableScanRule());
    opRules.put(new RuleRegExp("RS", ReduceSinkOperator.getOperatorName() + "%"),
        MetaInfoRulesProcFactory.getReduceSinkRule());
    opRules.put(new RuleRegExp("JOIN", JoinOperator.getOperatorName() + "%"), 
        MetaInfoRulesProcFactory.getMultiParentRule());
    opRules.put(new RuleRegExp("MUX", MuxOperator.getOperatorName() + "%"), 
        MetaInfoRulesProcFactory.getMultiParentRule());
    opRules.put(new RuleRegExp("DEMUX", DemuxOperator.getOperatorName() + "%"), 
        MetaInfoRulesProcFactory.getMultiParentRule());
    opRules.put(new RuleRegExp("UNION", UnionOperator.getOperatorName() + "%"), 
        MetaInfoRulesProcFactory.getMultiParentRule());
    opRules.put(new RuleRegExp("GBY", GroupByOperator.getOperatorName() + "%"), 
        MetaInfoRulesProcFactory.getGroupByRule());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(MetaInfoRulesProcFactory.getDefaultRule(), opRules,
        annotateCtx);
    GraphWalker ogw = new PreOrderWalker(disp);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getTopOps().values());
    ogw.startWalking(topNodes, null);

    return pctx;
  }

}
