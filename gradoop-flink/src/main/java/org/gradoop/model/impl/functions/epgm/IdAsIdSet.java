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

package org.gradoop.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.model.api.EPGMElement;
import org.gradoop.model.impl.id.GradoopIdSet;

/**
 * Maps an element to a GradoopIdSet, containing the elements id.
 *
 * @param <EL> element type
 */
@FunctionAnnotation.ReadFields("id")
public class IdAsIdSet<EL extends EPGMElement>
  implements MapFunction<EL, GradoopIdSet> {

  @Override
  public GradoopIdSet map(EL element) {
    return GradoopIdSet.fromExisting(element.getId());
  }
}
