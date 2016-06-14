package org.gradoop.model.impl.algorithms.fsm.miners.gspan.filterrefine.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos
  .GSpanGraph;

import java.util.Collection;

public class SearchSpacePartition extends RichMapPartitionFunction
  <GSpanGraph, Tuple2<Integer, Collection<GSpanGraph>>> {

  @Override
  public void mapPartition(Iterable<GSpanGraph> iterable,
    Collector<Tuple2<Integer, Collection<GSpanGraph>>> collector) throws
    Exception {

    int workerId = getRuntimeContext().getIndexOfThisSubtask();
    Collection<GSpanGraph> transactions = Lists.newArrayList(iterable);

    collector.collect(new Tuple2<>(workerId, transactions));
  }
}
