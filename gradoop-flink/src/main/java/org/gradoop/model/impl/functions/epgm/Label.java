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
import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.model.api.EPGMLabeled;

/**
 * labeled EPGM element => label
 *
 * @param <L> EPGM labeled type
 */
@FunctionAnnotation.ForwardedFields("label->*")
public class Label<L extends EPGMLabeled>
  implements MapFunction<L, String>, KeySelector<L, String> {

  @Override
  public String map(L l) throws Exception {
    return l.getLabel();
  }

  @Override
  public String getKey(L l) throws Exception {
    return l.getLabel();
  }
}
