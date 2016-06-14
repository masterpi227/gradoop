package org.gradoop.model.impl.algorithms.fsm.miners.gspan.filterrefine.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.GSpanGraph;

import java.util.Collection;


public class WorkerIdGraphCount
  implements MapFunction
  <Tuple2<Integer, Collection<GSpanGraph>>, Tuple2<Integer, Integer>> {

  @Override
  public Tuple2<Integer, Integer> map(
    Tuple2<Integer, Collection<GSpanGraph>> pair) throws Exception {
    return new Tuple2<>(pair.f0, pair.f1.size());
  }
}
