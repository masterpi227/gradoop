package org.gradoop.model.impl.algorithms.fsm;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMElement;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.algorithms.fsm.common.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.common.gspan.GSpan;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.*;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.tuples.GraphTransaction;

import java.util.*;


public class MinDfsCode
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements MapFunction<GraphTransaction<G, V, E>, DfsCode> {

  private final FSMConfig fsmConfig;

  public MinDfsCode(FSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  @Override
  public DfsCode map(GraphTransaction<G, V, E> graphTransaction
  ) throws Exception {
    List<String> vertexLabelDictionary = createDictionary
      (graphTransaction.getVertices());

    List<String> edgeLabelDictionary = createDictionary
      (graphTransaction.getEdges());

    Map<GradoopId, Integer> vertexMap = Maps.newHashMap();
    List<AdjacencyList> adjacencyLists = Lists.newArrayList();

    encodeVertices(graphTransaction.getVertices(),
      vertexLabelDictionary, vertexMap, adjacencyLists);

    Map<DfsCode, Collection<DFSEmbedding>> codeEmbeddings =
      Maps.newHashMap();

    encodeEdges(codeEmbeddings,
      graphTransaction.getEdges(), edgeLabelDictionary, vertexMap, adjacencyLists);

    Collection<Set<DfsCode>> codeSiblings =
      Lists.newArrayList();

    codeSiblings.add(Sets.newHashSet(codeEmbeddings.keySet()));

    GSpanTransaction transaction =
      new GSpanTransaction(adjacencyLists, codeEmbeddings);

    return GSpan.getMinimumDFSCode(transaction, fsmConfig);
  }

  private void encodeEdges(
    Map<DfsCode, Collection<DFSEmbedding>> codeEmbeddings,
    Set<E> edges,
    List<String> edgeLabelDictionary, Map<GradoopId, Integer> vertexMap,
    List<AdjacencyList> adjacencyLists) {
    int edgeId = 0;
    for(E edge : edges) {

      int sourceId = vertexMap.get(edge.getSourceId());
      AdjacencyList sourceAdjacencyList = adjacencyLists.get(sourceId);
      Integer sourceLabel = sourceAdjacencyList.getFromVertexLabel();

      int targetId = vertexMap.get(edge.getTargetId());
      AdjacencyList targetAdjacencyList = adjacencyLists.get(targetId);
      Integer targetLabel = targetAdjacencyList.getFromVertexLabel();

      int label = edgeLabelDictionary.indexOf(edge.getLabel());

      sourceAdjacencyList.getEntries().add(new AdjacencyListEntry(true, edgeId, label, targetId, targetLabel));

      targetAdjacencyList.getEntries().add(new AdjacencyListEntry(false, edgeId, label, sourceId, sourceLabel));

      DfsStep step;
      List<Integer> vertexTimes;

      if(sourceId == targetId) {
        step = new DfsStep(0, sourceLabel, true, label, 0, targetLabel);
        vertexTimes = Lists.newArrayList(sourceId);
      } else if (sourceLabel <= targetLabel) {
        step = new DfsStep(0, sourceLabel, true, label, 1, targetLabel);
        vertexTimes = Lists.newArrayList(sourceId, targetId);
      } else {
        step = new DfsStep(0, targetLabel, false, label, 1, sourceLabel);
        vertexTimes = Lists.newArrayList(targetId, sourceId);
      }

      DfsCode code = new DfsCode(step);
      DFSEmbedding embedding =
        new DFSEmbedding(vertexTimes, Lists.newArrayList(edgeId));

      Collection<DFSEmbedding> embeddings = codeEmbeddings.get(code);

      if(embeddings == null) {
        codeEmbeddings.put(code, Lists.newArrayList(embedding));
      } else {
        embeddings.add(embedding);
      }

      edgeId++;
    }
  }

  private void encodeVertices(Set<V> vertices,
    List<String> vertexLabelDictionary, Map<GradoopId, Integer> vertexMap,
    List<AdjacencyList> adjacencyLists) {
    int vertexId = 0;
    for(V vertex : vertices) {
      vertexMap.put(vertex.getId(), vertexId);

      adjacencyLists.add(vertexId, new AdjacencyList(vertexLabelDictionary
        .indexOf(vertex.getLabel())));

      vertexId++;
    }
  }

  private <EL extends EPGMElement> List<String> createDictionary(
    Set<EL> elements) {

    Map<String, Integer> labelCount = Maps.newHashMap();

    for (EL element : elements) {
      String label = element.getLabel();
      Integer count = labelCount.get(label);

      if(count == null) {
        count = 1;
      } else {
        count = count + 1;
      }

      labelCount.put(label, count);
    }

    List<Map.Entry<String, Integer>> entries =
      Lists.newArrayList(labelCount.entrySet());

    Collections.sort(entries, new DictionaryEntryComparator());

    List<String> dictionary = Lists.newArrayList();

    for(Map.Entry<String, Integer> entry : entries) {
      dictionary.add(entry.getKey());
    }

    return dictionary;
  }

  private class DictionaryEntryComparator implements
    java.util.Comparator<Map.Entry<String, Integer>> {

    @Override
    public int compare(Map.Entry<String, Integer> o1,
      Map.Entry<String, Integer> o2) {
      int comparison = o1.getValue() - o2.getValue();

      if(comparison == 0) {
        comparison = o1.getKey().compareTo(o2.getKey());
      }

      return comparison;
    }
  }
}