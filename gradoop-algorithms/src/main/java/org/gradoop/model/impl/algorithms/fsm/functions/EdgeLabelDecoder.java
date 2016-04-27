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

package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.id.GradoopId;

import java.util.ArrayList;

/**
 * (graphId, sourceIdId, targetId, integerLabel) |><| (integerLabel,stringLabel)
 * => (graphId, sourceIdId, targetId, stringLabel)
 */
public class EdgeLabelDecoder extends RichMapFunction<
  Tuple4<GradoopId, GradoopId, GradoopId, Integer>,
    Tuple4<GradoopId, GradoopId, GradoopId, String>> {

  public static final String DICTIONARY = "dictionary";
  private ArrayList<String> dictionary;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.dictionary = getRuntimeContext()
      .<ArrayList<String>>getBroadcastVariable(DICTIONARY)
      .get(0);
  }

  @Override
  public Tuple4<GradoopId, GradoopId, GradoopId, String> map(
    Tuple4<GradoopId, GradoopId, GradoopId, Integer> edge) throws
    Exception {

    return new Tuple4<>(edge.f0, edge.f1, edge.f2, dictionary.get(edge.f3));
  }
}
