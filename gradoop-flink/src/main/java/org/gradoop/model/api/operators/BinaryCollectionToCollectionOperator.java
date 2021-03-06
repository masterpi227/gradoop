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

package org.gradoop.model.api.operators;

import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.operators.difference.Difference;
import org.gradoop.model.impl.operators.intersection.Intersection;
import org.gradoop.model.impl.operators.union.Union;

/**
 * Creates a {@link GraphCollection} based on two input collections.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 * @see Union
 * @see Intersection
 * @see Difference
 */
public interface BinaryCollectionToCollectionOperator<
  G extends EPGMGraphHead,
  V extends EPGMVertex,
  E extends EPGMEdge>
  extends Operator {
  /**
   * Executes the operator.
   *
   * @param firstCollection  first input collection
   * @param secondCollection second input collection
   * @return operator result
   */
  GraphCollection<G, V, E> execute(GraphCollection<G, V, E> firstCollection,
    GraphCollection<G, V, E> secondCollection);
}
