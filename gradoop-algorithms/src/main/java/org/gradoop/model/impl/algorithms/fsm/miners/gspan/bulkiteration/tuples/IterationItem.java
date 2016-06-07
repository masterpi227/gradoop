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

package org.gradoop.model.impl.algorithms.fsm.miners.gspan.bulkiteration.tuples;

import org.apache.commons.lang.StringUtils;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.CompressedSubgraph;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.GSpanTransaction;
import org.gradoop.model.impl.tuples.WithCount;

import java.io.Serializable;
import java.util.Collection;

public class IterationItem implements Serializable {


  private final GSpanTransaction transaction;
  private final Collection<WithCount<CompressedSubgraph>> frequentSubgraphs;

  public IterationItem(GSpanTransaction transaction) {
    this.transaction = transaction;
    this.frequentSubgraphs = null;
  }

  public IterationItem(Collection<WithCount<CompressedSubgraph>> frequentSubgraphs) {
    this.transaction = null;
    this.frequentSubgraphs = frequentSubgraphs;
  }


  public boolean isTransaction() {
    return transaction != null;
  }

  public boolean isCollector() {
    return frequentSubgraphs != null;
  }

  public GSpanTransaction getTransaction() {
    return this.transaction;
  }

  public Collection<WithCount<CompressedSubgraph>> getFrequentSubgraphs() {
    return this.frequentSubgraphs;
  }

  @Override
  public String toString() {
    return isCollector() ?
      "Collector:\n" +  StringUtils.join(frequentSubgraphs, "\n") :
      "Transaction:\n" + transaction.toString();
  }
}