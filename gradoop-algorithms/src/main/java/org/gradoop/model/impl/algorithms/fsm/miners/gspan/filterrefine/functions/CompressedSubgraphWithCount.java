package org.gradoop.model.impl.algorithms.fsm.miners.gspan.filterrefine.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.CompressedDFSCode;
import org.gradoop.model.impl.tuples.WithCount;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.filterrefine.tuples
  .RefinementMessage;

/**
 * Created by peet on 02.06.16.
 */
public class CompressedSubgraphWithCount
  implements MapFunction<RefinementMessage, WithCount<CompressedDFSCode>> {

  @Override
  public WithCount<CompressedDFSCode> map(RefinementMessage message) throws Exception {
    return new WithCount<>(message.getSubgraph(), message.getSupport());
  }
}
