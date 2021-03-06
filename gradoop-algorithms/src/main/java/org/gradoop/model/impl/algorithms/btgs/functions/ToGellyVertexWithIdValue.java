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

package org.gradoop.model.impl.algorithms.btgs.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.id.GradoopId;

/**
 * EPGM vertex to gelly vertex, where value is vertex id
 * @param <V> EPGM vertex type
 */
public class ToGellyVertexWithIdValue<V extends EPGMVertex>
  implements MapFunction<V, Vertex<GradoopId, GradoopId>> {

  @Override
  public Vertex<GradoopId, GradoopId> map(V vertex) throws Exception {
    GradoopId id = vertex.getId();
    return new Vertex<>(id, id);
  }
}
