package org.gradoop.model.impl.algorithms.fsm.miners;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.model.impl.algorithms.fsm.config.FsmConfig;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.CompressedDFSCode;
import org.gradoop.model.impl.tuples.WithCount;
import org.gradoop.model.impl.algorithms.fsm.encoders.tuples.EdgeTriple;


public interface TransactionalFsMiner {

  DataSet<WithCount<CompressedDFSCode>> mine(DataSet<EdgeTriple> edges,
    DataSet<Integer> minSupport, FsmConfig fsmConfig);

  void setExecutionEnvironment(ExecutionEnvironment env);
}
