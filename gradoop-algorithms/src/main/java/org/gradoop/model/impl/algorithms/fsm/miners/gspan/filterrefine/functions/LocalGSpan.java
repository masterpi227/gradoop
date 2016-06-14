package org.gradoop.model.impl.algorithms.fsm.miners.gspan.filterrefine.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.config.FsmConfig;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.GSpan;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.DFSCode;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos
  .GSpanGraph;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.CompressedDFSCode;
import org.gradoop.model.impl.tuples.WithCount;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.filterrefine.tuples.FilterResult;

import java.util.Collection;
import java.util.Map;

public class LocalGSpan implements FlatMapFunction
  <Tuple2<Integer, Collection<GSpanGraph>>, FilterResult> {

  private final FsmConfig fsmConfig;

  public LocalGSpan(FsmConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  @Override
  public void flatMap(Tuple2<Integer, Collection<GSpanGraph>> pair,
    Collector<FilterResult> collector
  ) throws Exception {
    Collection<GSpanGraph> transactions = pair.f1;

    int graphCount = transactions.size();
    int minSupport = (int) (fsmConfig.getThreshold() * (float) graphCount) - 1;
    int minLikelySupport =
      (int) (fsmConfig.getLikelinessThreshold() * (float) graphCount) - 1;

    Collection<WithCount<DFSCode>> allLocallyFrequentSubgraphs =
      Lists.newArrayList();
    Collection<WithCount<DFSCode>> likelyFrequentSubgraphs =
      Lists.newArrayList();
    Collection<WithCount<DFSCode>> currentFrequentSubgraphs = null;

    int edgeCount = 1;
    do {
      // count support
      Map<DFSCode, Integer> codeSupport = countSupport(transactions);

      currentFrequentSubgraphs = Lists.newArrayList();

      for (Map.Entry<DFSCode, Integer> entry : codeSupport.entrySet())
      {
        DFSCode code = entry.getKey();
        int support = entry.getValue();

        if (support >= minSupport) {
          if(GSpan.isMinimumDfsCode(code, fsmConfig)) {
            WithCount<DFSCode> supportable = new WithCount<>(code, support);
            currentFrequentSubgraphs.add(supportable);
            allLocallyFrequentSubgraphs.add(supportable);
          }
        } else if (support >= minLikelySupport) {
          if (GSpan.isMinimumDfsCode(code, fsmConfig)) {
            likelyFrequentSubgraphs.add(new WithCount<>(code, support));
          }
        }
      }

      for (GSpanGraph transaction : transactions) {
        if (transaction.hasGrownSubgraphs()) {
          GSpan.growEmbeddings(
            transaction, unwrap(currentFrequentSubgraphs), fsmConfig);
        }
      }

      edgeCount++;
    } while (! currentFrequentSubgraphs.isEmpty()
      && edgeCount <= fsmConfig.getMaxEdgeCount());

    collect(collector, pair.f0,
      allLocallyFrequentSubgraphs, likelyFrequentSubgraphs);
  }

  private Collection<DFSCode> unwrap(Collection<WithCount<DFSCode>> wrappedCodes) {

    Collection<DFSCode> codes = Lists.newArrayListWithExpectedSize(wrappedCodes.size());

    for (WithCount<DFSCode> wrappedCode : wrappedCodes) {
      codes.add(wrappedCode.getObject());
    }

    return codes;
  }

  private Map<DFSCode, Integer> countSupport(
    Collection<GSpanGraph> transactions) {

    Map<DFSCode, Integer> codeSupport = Maps.newHashMap();

    for (GSpanGraph transaction : transactions) {
      if (transaction.hasGrownSubgraphs()) {
        for (DFSCode code : transaction.getCodeEmbeddings().keySet()) {

          Integer support = codeSupport.get(code);
          support = support == null ? 1 : support + 1;

          codeSupport.put(code, support);
        }
      }
    }

    return codeSupport;
  }


  private void collect(
    Collector<FilterResult> collector,
    int workerId, Collection<WithCount<DFSCode>> locallyFrequentDfsCodes,
    Collection<WithCount<DFSCode>> likelyFrequentDfsCodes) {
    for(WithCount<DFSCode> subgraph : locallyFrequentDfsCodes)
    {
      collector.collect(new FilterResult(
        new CompressedDFSCode(subgraph.getObject()),
        subgraph.getCount(), workerId, true));
    }
    for(WithCount<DFSCode> subgraph : likelyFrequentDfsCodes)
    {
      collector.collect(new FilterResult(
        new CompressedDFSCode(subgraph.getObject()),
        subgraph.getCount(), workerId, false));
    }
  }
}
