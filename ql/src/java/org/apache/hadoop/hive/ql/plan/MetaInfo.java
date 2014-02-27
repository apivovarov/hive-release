package org.apache.hadoop.hive.ql.plan;

import java.util.List;

import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;

public class MetaInfo {
  
  List<ExprNodeDesc> keyCols;
  Table table;
  PrunedPartitionList prunedPartList;
  
  public MetaInfo(List<ExprNodeDesc> keyCols, Table table, PrunedPartitionList prunedPartList) {
    this.keyCols = keyCols;
    this.table = table;
    this.prunedPartList = prunedPartList;
  }

  public List<ExprNodeDesc> getKeyCols() {
    return keyCols;
  }

  public Table getTable() {
    return table;
  }

  public PrunedPartitionList getPrunedPartList() {
    return prunedPartList;
  }
}
