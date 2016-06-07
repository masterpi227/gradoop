package org.gradoop.model.impl.algorithms.fsm.miners.gspan.filterrefine.functions;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.config.FsmConfig;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.GSpan;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.DfsCode;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.GSpanTransaction;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.CompressedSubgraph;
import org.gradoop.model.impl.tuples.WithCount;

import java.util.Collection;

public class Refinement implements FlatJoinFunction<
  Tuple2<Integer, Collection<CompressedSubgraph>>,
  Tuple2<Integer, Collection<GSpanTransaction>>, WithCount<CompressedSubgraph>> {


  private final FsmConfig fsmConfig;

  public Refinement(FsmConfig config) {
    fsmConfig = config;
  }

  @Override
  public void join(
    Tuple2<Integer, Collection<CompressedSubgraph>> partitionSubgraphs,
    Tuple2<Integer, Collection<GSpanTransaction>> partitionGraphs,
    Collector<WithCount<CompressedSubgraph>> collector) throws Exception {

    Collection<CompressedSubgraph> refinementSubgraphs = partitionSubgraphs.f1;
    Collection<GSpanTransaction> graphs = partitionGraphs.f1;

    for(CompressedSubgraph compressedSubgraph : refinementSubgraphs) {

      DfsCode subgraph = compressedSubgraph.getDfsCode();
      int frequency = 0;

      for(GSpanTransaction graph : graphs) {
        if(GSpan.contains(graph, subgraph, fsmConfig)) {
          frequency++;
        }
      }

      if (frequency > 0) {
        WithCount<CompressedSubgraph> subgraphWithCount =
          new WithCount<>(compressedSubgraph, frequency);

        collector.collect(subgraphWithCount);
      }
    }
  }
}
