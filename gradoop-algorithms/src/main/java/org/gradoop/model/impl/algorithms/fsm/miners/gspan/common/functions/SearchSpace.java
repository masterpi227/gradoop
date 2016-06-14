package org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.GSpan;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos
  .GSpanGraph;
import org.gradoop.model.impl.algorithms.fsm.encoders.tuples.EdgeTriple;

public class SearchSpace
  implements GroupReduceFunction<EdgeTriple, GSpanGraph> {

  @Override
  public void reduce(Iterable<EdgeTriple> iterable,
    Collector<GSpanGraph> collector) throws Exception {

    collector.collect(GSpan.createGSpanGraph(iterable));
  }
}
