/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */


package org.gradoop.model.impl.algorithms.fsm.miners.gspan;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.model.impl.algorithms.fsm.config.FsmConfig;
import org.gradoop.model.impl.algorithms.fsm.encoders.tuples.EdgeTriple;
import org.gradoop.model.impl.algorithms.fsm.miners.TransactionalFsMiner;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.functions
  .SearchSpace;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos
  .GSpanGraph;

/**
 * Abstract superclass of Flink GSpan implementations.
 */
public abstract class GSpanBase implements TransactionalFsMiner {

  /**
   * Flink execution environment
   */
  protected ExecutionEnvironment env;

  /**
   * FSM configuration
   */
  protected FsmConfig fsmConfig;

  @Override
  public void setExecutionEnvironment(ExecutionEnvironment env) {
    this.env = env;
  }

  public void setFsmConfig(FsmConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  /**
   * Creates mining graph representation from edge triples.
   *
   * @param edges edges
   * @return graph transactions
   */
  protected DataSet<GSpanGraph> createTransactions(
    DataSet<EdgeTriple> edges) {
    return edges
      .groupBy(0)
      .reduceGroup(new SearchSpace());
  }
}
