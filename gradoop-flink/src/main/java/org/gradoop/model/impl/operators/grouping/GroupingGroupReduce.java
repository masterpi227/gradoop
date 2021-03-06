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

package org.gradoop.model.impl.operators.grouping;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.operators.grouping.functions.BuildVertexGroupItem;
import org.gradoop.model.impl.operators.grouping.functions.FilterNonCandidates;
import org.gradoop.model.impl.operators.grouping.functions.FilterCandidates;
import org.gradoop.model.impl.operators.grouping.functions.BuildSuperVertex;
import org.gradoop.model.impl.operators.grouping.functions.BuildVertexWithRepresentative;
import org.gradoop.model.impl.operators.grouping.functions.ReduceVertexGroupItems;
import org.gradoop.model.impl.operators.grouping.functions.aggregation.PropertyValueAggregator;

import org.gradoop.model.impl.operators.grouping.tuples.EdgeGroupItem;
import org.gradoop.model.impl.operators.grouping.tuples.VertexGroupItem;
import org.gradoop.model.impl.operators.grouping.tuples.VertexWithRepresentative;

import java.util.List;

/**
 * Grouping implementation that does not require sorting of vertex groups.
 *
 * Algorithmic idea:
 *
 * 1) Map vertices to a minimal representation, i.e. {@link VertexGroupItem}.
 * 2) Group vertices on label and/or property.
 * 3) Create a group representative for each group and collect a non-candidate
 *    {@link VertexGroupItem} for each group element and one additional
 *    candidate {@link VertexGroupItem} that holds the group aggregate.
 * 4) Filter output of 3)
 *    a) non-candidate tuples are mapped to {@link VertexWithRepresentative}
 *    b) candidate tuples are used to build final summarized vertices
 * 5) Map edges to a minimal representation, i.e. {@link EdgeGroupItem}
 * 6) Join edges with output of 4a) and replace source/target id with group
 *    representative.
 * 7) Updated edges are grouped by source and target id and optionally by label
 *    and/or edge property.
 * 8) Group combine on the workers and compute aggregate.
 * 9) Group reduce globally and create final summarized edges.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class GroupingGroupReduce<
  G extends EPGMGraphHead,
  V extends EPGMVertex,
  E extends EPGMEdge>
  extends Grouping<G, V, E> {
  /**
   * Creates grouping operator instance.
   *
   * @param vertexGroupingKeys  property key to group vertices
   * @param useVertexLabels     group on vertex label true/false
   * @param vertexAggregators   aggregate functions for grouped vertices
   * @param edgeGroupingKeys    property key to group edges
   * @param useEdgeLabels       group on edge label true/false
   * @param edgeAggregators     aggregate functions for grouped edges
   */
  GroupingGroupReduce(
    List<String> vertexGroupingKeys,
    boolean useVertexLabels,
    List<PropertyValueAggregator> vertexAggregators,
    List<String> edgeGroupingKeys,
    boolean useEdgeLabels,
    List<PropertyValueAggregator> edgeAggregators) {
    super(
      vertexGroupingKeys, useVertexLabels, vertexAggregators,
      edgeGroupingKeys, useEdgeLabels, edgeAggregators);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected LogicalGraph<G, V, E> groupInternal(LogicalGraph<G, V, E> graph) {

    DataSet<VertexGroupItem> verticesForGrouping = graph.getVertices()
      // map vertex to vertex group item
      .map(new BuildVertexGroupItem<V>(
        getVertexGroupingKeys(), useVertexLabels(), getVertexAggregators()));

    DataSet<VertexGroupItem> vertexGroupItems =
      // group vertices by label / properties / both
      groupVertices(verticesForGrouping)
        // apply aggregate function
        .reduceGroup(new ReduceVertexGroupItems(
          useVertexLabels(), getVertexAggregators()));

    DataSet<V> superVertices = vertexGroupItems
      // filter group representative tuples
      .filter(new FilterCandidates())
      // build super vertices
      .map(new BuildSuperVertex<>(getVertexGroupingKeys(),
        useVertexLabels(), getVertexAggregators(), config.getVertexFactory()));

    DataSet<VertexWithRepresentative> vertexToRepresentativeMap =
      vertexGroupItems
        // filter group element tuples
        .filter(new FilterNonCandidates())
        // build vertex to group representative tuple
        .map(new BuildVertexWithRepresentative());

    // build super edges
    DataSet<E> superEdges = buildSuperEdges(graph, vertexToRepresentativeMap);

    return LogicalGraph.fromDataSets(
      superVertices, superEdges, graph.getConfig());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return GroupingGroupReduce.class.getName();
  }
}
