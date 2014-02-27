package org.apache.hadoop.hive.ql.exec.tez;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;

class CustomEdgeConfiguration implements Writable {

  Multimap<Integer, Integer> bucketToTaskMap = null;
  
  public CustomEdgeConfiguration() {
  }
  
  public CustomEdgeConfiguration(Multimap<Integer, Integer> routingTable) {
    this.bucketToTaskMap = routingTable;
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    // TODO Null checks etc if required...
    out.writeInt(bucketToTaskMap.size());

    for (Entry<Integer, Collection<Integer>> entry : bucketToTaskMap.asMap().entrySet()) {
      int bucketNum = entry.getKey();
      for (Integer taskId : entry.getValue()) {
        out.writeInt(bucketNum);
        out.writeInt(taskId);
      }
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int count = in.readInt();
    bucketToTaskMap = LinkedListMultimap.create();
    for (int i = 0; i < count; i++) {
      bucketToTaskMap.put(in.readInt(), in.readInt());
    }

    if (count != bucketToTaskMap.size()) {
      throw new IOException("Was not a clean translation. Some records are missing");
    }
  }

  public Multimap<Integer, Integer> getRoutingTable() {
    return bucketToTaskMap;
  }
}