package org.gradoop.model.impl.algorithms.fsm.miners.gspan.filterrefine.functions;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.config.FsmConfig;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.GSpan;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.DFSCode;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos
  .GSpanGraph;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.CompressedDFSCode;
import org.gradoop.model.impl.tuples.WithCount;

import java.util.Collection;

public class Refinement implements FlatJoinFunction<
  Tuple2<Integer, Collection<CompressedDFSCode>>,
  Tuple2<Integer, Collection<GSpanGraph>>, WithCount<CompressedDFSCode>> {


  private final FsmConfig fsmConfig;

  public Refinement(FsmConfig config) {
    fsmConfig = config;
  }

  @Override
  public void join(
    Tuple2<Integer, Collection<CompressedDFSCode>> partitionSubgraphs,
    Tuple2<Integer, Collection<GSpanGraph>> partitionGraphs,
    Collector<WithCount<CompressedDFSCode>> collector) throws Exception {

    Collection<CompressedDFSCode> refinementSubgraphs = partitionSubgraphs.f1;
    Collection<GSpanGraph> graphs = partitionGraphs.f1;

    for(CompressedDFSCode compressedSubgraph : refinementSubgraphs) {

      DFSCode subgraph = compressedSubgraph.getDfsCode();
      int frequency = 0;

      for(GSpanGraph graph : graphs) {
        if(GSpan.contains(graph, subgraph, fsmConfig)) {
          frequency++;
        }
      }

      if (frequency > 0) {
        WithCount<CompressedDFSCode> subgraphWithCount =
          new WithCount<>(compressedSubgraph, frequency);

        collector.collect(subgraphWithCount);
      }
    }
  }
}
