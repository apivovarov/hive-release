package org.apache.hadoop.hive.ql.plan;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;

public class TezEdgeProperty {
  
  public enum EdgeType {
    SIMPLE_EDGE,
    BROADCAST_EDGE, 
    CONTAINS,
    CUSTOM_EDGE
  }

  private HiveConf hiveConf;
  private EdgeType edgeType;
  private Map<String, List<Integer>> routingTable;
  private int numBuckets;

  public TezEdgeProperty(HiveConf hiveConf, EdgeType edgeType, 
      Map<String, List<Integer>> routingTable, int buckets) {
    this.hiveConf = hiveConf;
    this.edgeType = edgeType;
    this.routingTable = routingTable;
    this.numBuckets = buckets;
  }

  public EdgeType getEdgeType() {
    return edgeType;
  }
  
  public Map<String, List<Integer>> getRoutingTable() {
    return routingTable;
  }
  
  public HiveConf getHiveConf () {
    return hiveConf;
  }

  public int getNumBuckets() {
    return numBuckets;
  }
}
