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
package org.gradoop.model.impl.operators.equality;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.BinaryGraphToValueOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.operators.tostring.api.EdgeToString;
import org.gradoop.model.impl.operators.tostring.api.GraphHeadToString;
import org.gradoop.model.impl.operators.tostring.api.VertexToString;

/**
 * Operator to determine if two graph are equal according to given string
 * representations of graph heads, vertices and edges.
 * @param <G> graph head type
 * @param <V> vertex type
 * @param <E> edge type
 */
public class GraphEquality
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements BinaryGraphToValueOperator<G, V, E, Boolean> {

  /**
   * collection equality operator, wrapped by graph equality
   * (graph are considered to be a 1-graph collection)
   */
  private final CollectionEquality<G, V, E> collectionEquality;
  /**
   * sets mode for directed or undirected graphs
   */
  private final boolean directed;

  /**
   * constructor to set string representations
   * @param graphHeadToString string representation of graph heads
   * @param vertexToString string representation of vertices
   * @param edgeToString string representation of edges
   * @param directed sets mode for directed or undirected graphs
   */
  public GraphEquality(GraphHeadToString<G> graphHeadToString,
    VertexToString<V> vertexToString, EdgeToString<E> edgeToString,
    boolean directed) {
    this.directed = directed;

    this.collectionEquality = new CollectionEquality<>(
      graphHeadToString, vertexToString, edgeToString, this.directed);
  }

  @Override
  public DataSet<Boolean> execute(
    LogicalGraph<G, V, E> firstGraph, LogicalGraph<G, V, E> secondGraph) {
    return collectionEquality.execute(
      GraphCollection.fromGraph(firstGraph),
      GraphCollection.fromGraph(secondGraph)
    );
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }
}
