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

package org.gradoop.model.impl.operators.exclusion;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.ReducibleBinaryGraphToGraphOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.epgm.ByDifferentId;
import org.gradoop.model.impl.functions.epgm.Id;
import org.gradoop.model.impl.functions.graphcontainment.InGraph;
import org.gradoop.model.impl.functions.graphcontainment.NotInGraphsBroadcast;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Computes the exclusion graph from a collection of logical graphs.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class ReduceExclusion
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements ReducibleBinaryGraphToGraphOperator<G, V, E> {

  /**
   * Graph identifier to start excluding from in a collection scenario.
   */
  private final GradoopId startId;

  /**
   * Creates an operator instance which can be applied on a graph collection. As
   * exclusion is not a commutative operation, a start graph needs to be set
   * from which the remaining graphs will be excluded.
   *
   * @param startId graph id from which other graphs will be exluded from
   */
  public ReduceExclusion(GradoopId startId) {
    this.startId = startId;
  }

  /**
   * Creates a new logical graph that contains only vertices and edges that
   * are contained in the starting graph but not in any other graph that is part
   * of the given collection.
   *
   * @param collection input collection
   * @return excluded graph
   */
  @Override
  public LogicalGraph<G, V, E> execute(GraphCollection<G, V, E> collection) {
    DataSet<GradoopId> excludedGraphIds = collection.getGraphHeads()
      .filter(new ByDifferentId<G>(startId))
      .map(new Id<G>());

    DataSet<V> vertices = collection.getVertices()
      .filter(new InGraph<V>(startId))
      .filter(new NotInGraphsBroadcast<V>())
      .withBroadcastSet(excludedGraphIds, NotInGraphsBroadcast.GRAPH_IDS);

    DataSet<E> edges = collection.getEdges()
      .filter(new InGraph<E>(startId))
      .filter(new NotInGraphsBroadcast<E>())
      .withBroadcastSet(excludedGraphIds, NotInGraphsBroadcast.GRAPH_IDS);

    return LogicalGraph.fromDataSets(
      vertices,
      edges,
      collection.getConfig()
    );
  }

  @Override
  public String getName() {
    return ReduceExclusion.class.getName();
  }
}
