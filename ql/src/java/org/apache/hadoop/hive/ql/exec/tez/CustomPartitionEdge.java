package org.apache.hadoop.hive.ql.exec.tez;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.tez.dag.api.EdgeManager;
import org.apache.tez.dag.api.EdgeManagerContext;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;

import com.google.common.collect.Multimap;

public class CustomPartitionEdge implements EdgeManager {

  private Multimap<Integer, Integer> routingTable = null;

  // used by the framework at runtime. initialize is the real initializer at runtime
  public CustomPartitionEdge() {  
  }

  @Override
  public int getNumDestinationTaskPhysicalInputs(int numSourceTasks, 
      int destinationTaskIndex) {
    return numSourceTasks;
  }

  @Override
  public int getNumSourceTaskPhysicalOutputs(int numDestinationTasks, 
      int sourceTaskIndex) {
    return routingTable.keySet().size();
  }

  @Override
  public int getNumDestinationConsumerTasks(int sourceTaskIndex, int numDestinationTasks) {
    return numDestinationTasks;
  }

  // called at runtime to initialize the custom edge.
  @Override
  public void initialize(EdgeManagerContext context) {
    byte[] payload = context.getUserPayload();
    if (payload == null) {
      return;
    }
    routingTable = null;
    // De-serialization code
    DataInputBuffer dib = new DataInputBuffer();
    dib.reset(payload, payload.length);
    CustomEdgeConfiguration conf = new CustomEdgeConfiguration();
    try {
      conf.readFields(dib);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    this.routingTable = conf.getRoutingTable();    
  }

  @Override
  public void routeDataMovementEventToDestination(DataMovementEvent event,
      int sourceTaskIndex, int numDestinationTasks, Map<Integer, List<Integer>> mapDestTaskIndices) {
    int srcIndex = event.getSourceIndex();
    List<Integer> destTaskIndices = new ArrayList<Integer>();
    destTaskIndices.addAll(routingTable.get(srcIndex));
    mapDestTaskIndices.put(new Integer(sourceTaskIndex), destTaskIndices);
  }

  @Override
  public void routeInputSourceTaskFailedEventToDestination(int sourceTaskIndex, 
      int numDestinationTasks, Map<Integer, List<Integer>> mapDestTaskIndices) {
    List<Integer> destTaskIndices = new ArrayList<Integer>();
    addAllDestinationTaskIndices(numDestinationTasks, destTaskIndices);
    mapDestTaskIndices.put(new Integer(sourceTaskIndex), destTaskIndices);
  }

  @Override
  public int routeInputErrorEventToSource(InputReadErrorEvent event, 
      int destinationTaskIndex) {
    return event.getIndex();
  }
  
  void addAllDestinationTaskIndices(int numDestinationTasks, List<Integer> taskIndices) {
    for(int i=0; i<numDestinationTasks; ++i) {
      taskIndices.add(new Integer(i));
    }
  }
}
