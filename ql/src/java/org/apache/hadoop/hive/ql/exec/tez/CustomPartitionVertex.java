package org.apache.hadoop.hive.ql.exec.tez;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.tez.dag.api.EdgeManagerDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.dag.api.VertexManagerPlugin;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitProto;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.events.RootInputConfigureVertexTasksEvent;
import org.apache.tez.runtime.api.events.RootInputDataInformationEvent;
import org.apache.tez.runtime.api.events.RootInputUpdatePayloadEvent;
import org.apache.tez.runtime.api.events.VertexManagerEvent;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;


/*
 * Only works with old mapred API
 * Will only work with a single MRInput for now.
 * No grouping for now
 */
public class CustomPartitionVertex implements VertexManagerPlugin {

  VertexManagerPluginContext context;
  
  private Multimap<Integer, Integer> bucketToTaskMap = HashMultimap.<Integer, Integer>create();
  private Multimap<Integer, FileSplit> bucketToInitialSplitMap = 
      ArrayListMultimap.<Integer, FileSplit>create();
  
  private RootInputConfigureVertexTasksEvent configureVertexTaskEvent;
  private List<RootInputDataInformationEvent> dataInformationEvents;
  private Map<Path, List<FileSplit>> pathFileSplitsMap = new TreeMap<Path, List<FileSplit>>();
  private int numBuckets = -1;
  
  public CustomPartitionVertex() {
  }
  
  @Override
  public void initialize(VertexManagerPluginContext context) {
    this.context = context; 
    ByteBuffer byteBuf = ByteBuffer.wrap(context.getUserPayload());
    this.numBuckets = byteBuf.getInt();
  }
  
  @Override
  public void onVertexStarted(Map<String, List<Integer>> completions) {
    int numTasks = context.getVertexNumTasks(context.getVertexName());
    List<Integer> scheduledTasks = new ArrayList<Integer>(numTasks);
    for (int i=0; i<numTasks; ++i) {
      scheduledTasks.add(new Integer(i));
    }
    context.scheduleVertexTasks(scheduledTasks);
  }
  
  @Override
  public void onSourceTaskCompleted(String srcVertexName, Integer attemptId) {
  }

  @Override
  public void onVertexManagerEventReceived(VertexManagerEvent vmEvent) {
  }
  
  // One call per root Input - and for now only one is handled.
  @Override
  public void onRootVertexInitialized(String inputName, InputDescriptor inputDescriptor,
      List<Event> events) {
    boolean dataInformationEventSeen = false;
    for (Event event : events) {
      if (event instanceof RootInputConfigureVertexTasksEvent) {
        // No tasks should have been started yet. Checked by initial state check.
        Preconditions.checkState(dataInformationEventSeen == false);
        Preconditions
            .checkState(
                context.getVertexNumTasks(context.getVertexName()) == -1,
                "Parallelism for the vertex should be set to -1 if the InputInitializer is setting parallelism");
        RootInputConfigureVertexTasksEvent cEvent = (RootInputConfigureVertexTasksEvent) event;
        
        // The vertex cannot be configured until all DataEvents are seen - to build the routing table.
        configureVertexTaskEvent = cEvent;
        // TODO HIVE Eventually: This only works because grouping is not being done in the first impl
        dataInformationEvents = Lists.newArrayListWithCapacity(configureVertexTaskEvent.getNumTasks());
      }
      if (event instanceof RootInputUpdatePayloadEvent) {
        // No tasks should have been started yet. Checked by initial state check.
        Preconditions.checkState(dataInformationEventSeen == false);
        inputDescriptor.setUserPayload(((RootInputUpdatePayloadEvent) event).getUserPayload());
      } else if (event instanceof RootInputDataInformationEvent) {
        dataInformationEventSeen = true;
        RootInputDataInformationEvent diEvent = (RootInputDataInformationEvent) event;
        // Need to see all events to group them (will eventually be required)
        dataInformationEvents.add(diEvent);
        FileSplit fileSplit;
        try {
          fileSplit = getFileSplitFromEvent(diEvent);
        } catch (IOException e) {
          throw new RuntimeException("Failed to get file split for event: " + diEvent);
        }
        List<FileSplit> fsList = pathFileSplitsMap.get(fileSplit.getPath()); 
        if (fsList == null) {
          fsList = new ArrayList<FileSplit>();
        }
        fsList.add(fileSplit);
        pathFileSplitsMap.put(fileSplit.getPath(), fsList);
      }
    }

    findBucketNumForPath();

    Preconditions.checkState(dataInformationEvents.size() == configureVertexTaskEvent.getNumTasks(), 
        "Missing or too many DataInformationEvents");
    
    try {
      processAllEvents(inputName);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  private void processAllEvents(String inputName) throws IOException {
    int taskCount = 0;
    for (Entry<Integer, Collection<FileSplit>> entry : bucketToInitialSplitMap.asMap().entrySet()) {
      int bucketNum = entry.getKey();
      Collection<FileSplit> initialSplits = entry.getValue();
      // TODO Hive : Group all events belonging to a single bucket
      // TODO Hive: Figure out numTasks based on the grouping. For now 1 split per task
      for (int i = 0; i < initialSplits.size(); i++) {
        bucketToTaskMap.put(bucketNum, taskCount);
        taskCount++;
      }
    }

    // TODO HIVE: Figure out if this will be in payload, or whether it's safe to just iterate over the list provided by the VertexContext
    String dynPartInputName;

    Map<String, EdgeManagerDescriptor> emMap = Maps.newHashMap();
    Multimap<String, RootInputDataInformationEvent> perEdgeEvents = LinkedListMultimap.create();
    for (Map.Entry<String, EdgeProperty> entry : context
        .getInputVertexEdgeProperties().entrySet()) {
      if (entry.getValue().getDataMovementType() == DataMovementType.CUSTOM) {
        dynPartInputName = entry.getKey();
      } else {
        continue;
      }

      EdgeManagerDescriptor hiveEdgeManagerDesc = 
        new EdgeManagerDescriptor(CustomPartitionEdge.class.getName());

      byte[] payload = getBytePayload(bucketToTaskMap);
      hiveEdgeManagerDesc.setUserPayload(payload);
      // Configure # tasks and locations for Vertices
      emMap.put(dynPartInputName, hiveEdgeManagerDesc);

      // Send the actual splits for the tasks belonging to the Vertex.
      // TODO HIVE Eventually : Serialize the Grouped splits into events.
      // For now sending the inital events directly.
      for (RootInputDataInformationEvent event : dataInformationEvents) {
        event.setTargetIndex(event.getSourceIndex());
        perEdgeEvents.put(inputName, event);
      }
    }
    
    // TODO HIVE Eventually : Change numTasks and location hints to work off
    // of the Groups
    // Replace the Edge Managers
    context.setVertexParallelism(
        configureVertexTaskEvent.getNumTasks(),
        new VertexLocationHint(configureVertexTaskEvent
            .getTaskLocationHints()), emMap);
    
    for (Entry<String, Collection<RootInputDataInformationEvent>> entry : 
      perEdgeEvents.asMap().entrySet()) {
      context.addRootInputEvents(entry.getKey(), entry.getValue());
    }
  }

  private byte[] getBytePayload(Multimap<Integer, Integer> routingTable) throws IOException {
    CustomEdgeConfiguration edgeConf = new CustomEdgeConfiguration(routingTable);
    DataOutputBuffer dob = new DataOutputBuffer();
    edgeConf.write(dob);
    byte[] serialized = dob.getData();

    return serialized;
  }
  
  private FileSplit getFileSplitFromEvent(RootInputDataInformationEvent event) throws IOException {
    MRSplitProto splitProto = MRSplitProto.parseFrom(event.getUserPayload());
    SerializationFactory serializationFactory = new SerializationFactory(new Configuration());
    InputSplit inputSplit = MRHelpers.createOldFormatSplitFromUserPayload(splitProto,
        serializationFactory);
    if (!(inputSplit instanceof FileSplit)) {
      throw new UnsupportedOperationException(
          "Cannot handle splits other than FileSplit for the moment");
    }
    return (FileSplit) inputSplit;
  }
  
  private void findBucketNumForPath() {
    int bucketNum = 0;
    for (Map.Entry<Path, List<FileSplit>> entry : pathFileSplitsMap.entrySet()) {
      int bucketId = bucketNum % numBuckets;
      for (FileSplit fsplit : entry.getValue()) {
        bucketToInitialSplitMap.put(bucketId, fsplit);
      }
      bucketNum++;
    }
  }
}
