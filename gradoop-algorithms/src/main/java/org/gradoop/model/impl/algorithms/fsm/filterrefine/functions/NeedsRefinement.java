package org.gradoop.model.impl.algorithms.fsm.filterrefine.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.model.impl.algorithms.fsm.filterrefine.tuples.FilterResult;
import org.gradoop.model.impl.algorithms.fsm.filterrefine.tuples
  .RefinementMessage;

/**
 * Created by peet on 09.05.16.
 */
public class NeedsRefinement implements FilterFunction<RefinementMessage> {

  @Override
  public boolean filter(RefinementMessage message) throws Exception {
    return message.getMessageType() != RefinementMessage.GLOBALLY_FREQUENT;
  }
}