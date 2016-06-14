package org.gradoop.model.impl.algorithms.fsm.miners.gspan.filterrefine.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.filterrefine.tuples.RefinementMessage;

import java.util.Collection;
import java.util.Iterator;

public class RefinementCalls implements
  GroupReduceFunction<RefinementMessage, Tuple2<Integer, Collection<CompressedDFSCode>>> {

  @Override
  public void reduce(
    Iterable<RefinementMessage> iterable,
    Collector<Tuple2<Integer, Collection<CompressedDFSCode>>> collector) throws
    Exception {

    Iterator<RefinementMessage> iterator = iterable.iterator();

    RefinementMessage message = iterator.next();

    int workerId = message.getWorkerId();

    Collection<CompressedDFSCode> codes = Lists
      .newArrayList(message.getSubgraph());

    while (iterator.hasNext()) {
      message = iterator.next();
      codes.add(message.getSubgraph());
    }
    collector.collect(new Tuple2<>(workerId, codes));

  }

}
