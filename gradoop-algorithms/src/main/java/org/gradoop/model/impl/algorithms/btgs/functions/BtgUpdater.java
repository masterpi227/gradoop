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

import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Update Function of vertex centric iteration.
 */
public class BtgUpdater
  extends VertexUpdateFunction<GradoopId, GradoopId, GradoopId> {

  @Override
  public void updateVertex(Vertex<GradoopId, GradoopId> vertex,
    MessageIterator<GradoopId> messageIterator) throws Exception {

    GradoopId lastComponent = vertex.getValue();
    GradoopId newComponent = lastComponent;

    for (GradoopId messageComponent : messageIterator) {

      if (messageComponent.compareTo(newComponent) < 0) {
        newComponent = messageComponent;
      }
    }

    if (!lastComponent.equals(newComponent)) {
      setNewVertexValue(newComponent);
    }
  }
}
