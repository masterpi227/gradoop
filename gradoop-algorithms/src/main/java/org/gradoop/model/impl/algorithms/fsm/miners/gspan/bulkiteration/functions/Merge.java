package org.gradoop.model.impl.algorithms.fsm.miners.gspan.bulkiteration.functions;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.CompressedSubgraph;
import org.gradoop.model.impl.tuples.WithCount;

import java.util.Collection;

public class Merge implements ReduceFunction<Collection<WithCount<CompressedSubgraph>>> {

  @Override
  public Collection<WithCount<CompressedSubgraph>> reduce(
    Collection<WithCount<CompressedSubgraph>> firstCollection,
    Collection<WithCount<CompressedSubgraph>> secondCollection) throws Exception {


    Collection<WithCount<CompressedSubgraph>> mergedCollection;

    if(firstCollection.size() >= firstCollection.size()) {
      firstCollection.addAll(secondCollection);
      mergedCollection = firstCollection;
    } else {
      secondCollection.addAll(firstCollection);
      mergedCollection = secondCollection;
    }

//    System.out.println(firstCollection +
//      "\n+" + secondCollection +
//      "\n=" + mergedCollection);

    return mergedCollection;
  }
}