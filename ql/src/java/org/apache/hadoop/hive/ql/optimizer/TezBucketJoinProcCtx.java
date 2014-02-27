package org.apache.hadoop.hive.ql.optimizer;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;

public class TezBucketJoinProcCtx extends BucketJoinProcCtx {
  boolean isSubQuery = false;
  private Map<String, List<Integer>> routingTable;
  int numBuckets = -1;
  
  public TezBucketJoinProcCtx(HiveConf conf) {
    super(conf);
  }
  
  public void setIsSubQuery (boolean isSubQuery) {
    this.isSubQuery = isSubQuery;
  }
  
  public boolean isSubQuery () {
    return isSubQuery;
  }
  
  public void setRoutingTable (Map<String, List<Integer>>routingTable) {
    this.routingTable = routingTable;
  }
  
  public Map<String, List<Integer>> getRoutingTable() {
    return routingTable;
  }

  public void setNumBuckets(int numBuckets) {
    this.numBuckets = numBuckets;
  }

  public Integer getNumBuckets() {
    return numBuckets;
  }
}
