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

package org.gradoop.model.impl.operators.aggregation.functions.max;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.functions.AggregateFunction;
import org.gradoop.model.api.functions.ApplyAggregateFunction;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.properties.PropertyValue;

/**
 * Aggregate function returning the maximum of a specified property over all
 * vertices.
 *
 * @param <G> graph head type
 * @param <V> vertex type
 * @param <E> edge type
 */
public class MaxVertexProperty
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements
  AggregateFunction<G, V, E>, ApplyAggregateFunction<G, V, E> {

  /**
   * Property key to retrieve property values
   */
  private String propertyKey;

  /**
   * User defined maximum of same type as the property values,
   * needed to specify the type of the property
   */
  private Number maximum;

  /**
   * Constructor
   * @param propertyKey Property key to retrieve property values
   * @param maximum user defined maximum
   */
  public MaxVertexProperty(String propertyKey, Number maximum) {
    this.propertyKey = propertyKey;
    this.maximum = maximum;
  }

  /**
   * Returns a 1-element dataset containing the maximum of the given property
   * over the given elements.
   *
   * @param graph input graph
   * @return 1-element dataset with vertex count
   */
  @Override
  public DataSet<PropertyValue> execute(LogicalGraph<G, V, E> graph) {
    return Max.max(graph.getVertices(), propertyKey, maximum);
  }

  /**
   * Returns a dataset containing graph identifiers and the corresponding edge
   * maximum.
   *
   * @param collection input graph collection
   * @return dataset with graph + edge count tuples
   */
  @Override
  public DataSet<Tuple2<GradoopId, PropertyValue>> execute(
    GraphCollection<G, V, E> collection) {
    return Max.groupBy(collection.getVertices(), propertyKey, maximum);
  }
}
