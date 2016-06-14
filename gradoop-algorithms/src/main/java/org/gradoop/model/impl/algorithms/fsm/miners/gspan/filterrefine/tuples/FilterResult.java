package org.gradoop.model.impl.algorithms.fsm.miners.gspan.filterrefine.tuples;

import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.CompressedDFSCode;

public class FilterResult
  extends Tuple4<CompressedDFSCode, Integer, Integer, Boolean> {

  /**
   * as Filter result:
   * (subgraph, support, workerId, locally frequent)
   *
   */
  public FilterResult() {

  }

  public FilterResult(CompressedDFSCode subgraph, int support, int workerId, boolean locallyFrequent) {
    super(subgraph, support, workerId, locallyFrequent);
  }

  // FILTER RESULT

  public CompressedDFSCode getSubgraph() {
    return this.f0;
  }

  public int getSupport() {
    return this.f1;
  }

  public int getWorkerId() {
    return this.f2;
  }

  public boolean isLocallyFrequent() {
    return this.f3;
  }

  public boolean needsRefinement() {
    return !this.f3;
  }

}
