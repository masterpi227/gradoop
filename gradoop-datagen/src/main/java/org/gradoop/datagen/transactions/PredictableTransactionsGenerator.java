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

package org.gradoop.datagen.transactions;

import org.apache.flink.api.java.DataSet;
import org.gradoop.datagen.transactions.functions.PredictableTransaction;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.GraphTransactionsGenerator;
import org.gradoop.model.impl.GraphTransactions;
import org.gradoop.model.impl.tuples.GraphTransaction;
import org.gradoop.util.GradoopFlinkConfig;

/**
 * Data generator with predictable result for the evaluation of Frequent
 * Subgraph Mining algorithms.
 *
 *
 * @param <G>
 * @param <V>
 * @param <E>
 */
public class PredictableTransactionsGenerator
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements GraphTransactionsGenerator<G, V, E> {

  /**
   * Gradoop configuration
   */
  private final GradoopFlinkConfig<G, V, E> gradoopConfig;
  /**
   * specifies the number of generated graphs
   */
  private final long graphCount;
  /**
   * sets the minimum number of embeddings per subgraph pattern
   */
  private final int graphSize;

  public PredictableTransactionsGenerator(
    GradoopFlinkConfig<G, V, E> gradoopConfig, long graphCount, int graphSize) {
    this.gradoopConfig = gradoopConfig;
    this.graphCount = graphCount;
    this.graphSize = graphSize;
  }

  @Override
  public GraphTransactions<G, V, E> execute() {

    DataSet<Long> seeds = gradoopConfig
      .getExecutionEnvironment()
      .generateSequence(1, graphCount);

    DataSet<GraphTransaction<G, V, E>> transactions = seeds
      .map(new PredictableTransaction<>(
        gradoopConfig.getGraphHeadFactory(),
        gradoopConfig.getVertexFactory(),
        gradoopConfig.getEdgeFactory()
      ));

    return new GraphTransactions<>(transactions, gradoopConfig);
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }
}