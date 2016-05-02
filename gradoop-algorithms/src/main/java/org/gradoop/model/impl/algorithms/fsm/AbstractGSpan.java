/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.algorithms.fsm;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.algorithms.fsm.functions.*;
import org.gradoop.model.impl.algorithms.fsm.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.tuples.FatEdge;
import org.gradoop.model.impl.algorithms.fsm.tuples.VertexIdLabel;
import org.gradoop.model.impl.functions.join.LeftSide;
import org.gradoop.model.impl.functions.tuple.Project3To0And2;
import org.gradoop.model.impl.functions.tuple.Project4To0And3;
import org.gradoop.model.impl.functions.tuple.Value0Of3;
import org.gradoop.model.impl.functions.tuple.Value1Of2;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.count.Count;
import org.gradoop.util.GradoopFlinkConfig;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * abstract superclass of different implementations of the gSpan frequent
 * subgraph mining algorithm as Gradoop operator
 *
 * @param <G> graph type
 * @param <V> vertex type
 * @param <E> edge type
 */
public abstract class AbstractGSpan
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements UnaryCollectionToCollectionOperator<G, V, E> {

  /**
   * minimum support
   */
  protected DataSet minSupport;
  /**
   * frequent subgraph mining configuration
   */
  protected final FSMConfig fsmConfig;
  /**
   * Gradoop configuration
   */
  protected GradoopFlinkConfig<G, V, E> gradoopConfig;
  /**
   * edge label dictionary
   */
  protected DataSet<ArrayList<String>> edgeLabelDictionary;
  /**
   * vertex label dictionary
   */
  protected DataSet<ArrayList<String>> vertexLabelDictionary;

  /**
   * constructor
   * @param fsmConfig frequent subgraph mining configuration
   */
  public AbstractGSpan(FSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  /**
   * turns a data set of DFS codes into a graph collection
   * @param dfsCodes DFS code data set
   * @return graph collection
   */
  protected GraphCollection<G, V, E> decodeDfsCodes(
    DataSet<CompressedDFSCode> dfsCodes) {

    DataSet<Tuple3<G, ArrayList<Tuple2<GradoopId, Integer>>,
        ArrayList<Tuple3<GradoopId, GradoopId, Integer>>>> frequentSubgraphs =
      dfsCodes
        .map(new DfsDecoder<>(gradoopConfig.getGraphHeadFactory()));

    DataSet<G> graphHeads = frequentSubgraphs
      .map(new Value0Of3<G, ArrayList<Tuple2<GradoopId, Integer>>,
        ArrayList<Tuple3<GradoopId, GradoopId, Integer>>>());

    DataSet<V> vertices = frequentSubgraphs
      .flatMap(new ExpandVertices<G>())
      .map(new VertexLabelDecoder())
      .withBroadcastSet(vertexLabelDictionary, ExpandVertices.DICTIONARY)
      .map(new FullVertex<>(gradoopConfig.getVertexFactory()))
      .returns(gradoopConfig.getVertexFactory().getType());

    DataSet<E> edges = frequentSubgraphs
      .flatMap(new ExpandEdges<G>())
      .map(new EdgeLabelDecoder())
      .withBroadcastSet(edgeLabelDictionary, EdgeLabelDecoder.DICTIONARY)
      .map(new FullEdge<>(gradoopConfig.getEdgeFactory()))
      .returns(gradoopConfig.getEdgeFactory().getType());

    return GraphCollection.fromDataSets(
      graphHeads, vertices, edges, gradoopConfig);
  }

  /**
   * determines vertex label frequency and prunes by minimum support;
   * label frequencies are used to relabel vertices where higher support leads
   * to a smaller numeric label;
   *
   * @param collection input graph collection
   * @return pruned and relabelled edges
   */
  protected DataSet<VertexIdLabel>
  pruneAndRelabelVertices( GraphCollection<G, V, E> collection) {

    DataSet<V> vertices = collection.getVertices();

    vertexLabelDictionary = vertices
        .flatMap(new GraphIdVertexIdLabel<V>())
        .distinct()
        .map(new CountableLabel())
        .groupBy(0)
        .sum(1)
        .filter(new FrequentLabel())
        .withBroadcastSet(minSupport, Frequent.DS_NAME)
        .reduceGroup(new Dictionary());

    DataSet<HashMap<String, Integer>> stringIntDictionary =
      vertexLabelDictionary
        .map(new FlipDictionary());

    return vertices
      .flatMap(new VertexLabelEncoder<V>())
      .withBroadcastSet(stringIntDictionary, VertexLabelEncoder.DICTIONARY);
  }

  /**
   * determines edge label frequency and prunes by minimum support;
   * label frequencies are used to relabel edges where higher support leads
   * to a smaller numeric label;
   *
   * @param collection input graph collection
   * @return pruned and relabelled edges
   */
  protected DataSet<Tuple3<GradoopId, FatEdge, CompressedDFSCode>>
  pruneAndRelabelEdges(

    GraphCollection<G, V, E> collection) {

    DataSet<Tuple4<GradoopId, GradoopId, GradoopId, String>> gidSidTidLabel =
      collection
        .getEdges()
        .flatMap(new GraphIdSourceIdTargetIdLabel<E>());

    edgeLabelDictionary = gidSidTidLabel
      .map(new Project4To0And3<GradoopId, GradoopId, GradoopId, String>())
      .distinct()
      .map(new CountableLabel())
      .groupBy(0)
      .sum(1)
      .filter(new FrequentLabel())
      .withBroadcastSet(minSupport, Frequent.DS_NAME)
      .reduceGroup(new Dictionary());

    DataSet<HashMap<String, Integer>> stringIntDictionary = edgeLabelDictionary
      .map(new FlipDictionary());

    DataSet<VertexIdLabel> prunedVertices = pruneAndRelabelVertices(collection);

    return gidSidTidLabel
      .flatMap(new EdgeLabelEncoder())
      .withBroadcastSet(stringIntDictionary, EdgeLabelEncoder.DICTIONARY)
      .join(prunedVertices)
      .where(1).equalTo(0)
      .with(new AppendSourceLabel())
      .join(prunedVertices)
      .where(2).equalTo(0)
      .with(new AppendTargetLabelAndInitialDfsCode());
  }

  protected void setConfigAndMinSupport(GraphCollection<G, V, E> collection) {
    this.gradoopConfig = collection.getConfig();
    this.minSupport = Count
      .count(collection.getGraphHeads())
      .map(new MinSupport(fsmConfig.getThreshold()));
  }

  protected DataSet<CompressedDFSCode> find1EdgeFrequentDfsCodes(
    DataSet<Tuple3<GradoopId, FatEdge, CompressedDFSCode>> graphEdges) {

    return graphEdges
      .map(new Project3To0And2<GradoopId, FatEdge, CompressedDFSCode>())
      .distinct()
      .map(new Value1Of2<GradoopId, CompressedDFSCode>())
      .groupBy(0)
      .sum(1)
      .filter(new Frequent())
      .withBroadcastSet(minSupport, Frequent.DS_NAME);
  }

  protected DataSet<Tuple3<GradoopId, FatEdge, CompressedDFSCode>>
  filterFatEdges(
    DataSet<Tuple3<GradoopId, FatEdge, CompressedDFSCode>> fatEdges,
    DataSet<CompressedDFSCode> allFrequentDfsCodes) {

    return fatEdges
      .join(allFrequentDfsCodes)
      .where("2.0").equalTo(0)
      .with(new LeftSide<Tuple3<GradoopId, FatEdge, CompressedDFSCode>, CompressedDFSCode>());
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }
}
