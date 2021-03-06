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

package org.gradoop.io.impl.hbase;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.io.api.DataSource;
import org.gradoop.io.impl.hbase.inputformats.EdgeTableInputFormat;
import org.gradoop.io.impl.hbase.inputformats.GraphHeadTableInputFormat;
import org.gradoop.io.impl.hbase.inputformats.VertexTableInputFormat;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.GraphTransactions;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.tuple.ValueOf1;
import org.gradoop.model.impl.operators.combination.ReduceCombination;
import org.gradoop.storage.impl.hbase.HBaseEPGMStore;
import org.gradoop.util.GradoopFlinkConfig;

/**
 * Creates an EPGM instance from HBase.
 *
 * @param <G>  EPGM graph head type
 * @param <V>  EPGM vertex type
 * @param <E>  EPGM edge type
 */
public class HBaseDataSource<
  G extends EPGMGraphHead,
  V extends EPGMVertex,
  E extends EPGMEdge>
  extends HBaseBase<G, V, E>
  implements DataSource<G, V, E> {

  /**
   * Creates a new HBase data source.
   *
   * @param epgmStore HBase store
   * @param config    Gradoop Flink configuration
   */
  public HBaseDataSource(HBaseEPGMStore<G, V, E> epgmStore,
    GradoopFlinkConfig<G, V, E> config) {
    super(epgmStore, config);
  }

  @Override
  public LogicalGraph<G, V, E> getLogicalGraph() {
    return getGraphCollection().reduce(new ReduceCombination<G, V, E>());
  }

  @SuppressWarnings("unchecked")
  @Override
  public GraphCollection<G, V, E> getGraphCollection() {
    GradoopFlinkConfig<G, V, E> config = getFlinkConfig();
    HBaseEPGMStore<G, V, E> store = getStore();

    // used for type hinting when loading graph data
    TypeInformation<Tuple1<G>> graphTypeInfo = new TupleTypeInfo(
      Tuple1.class,
      TypeExtractor.createTypeInfo(config.getGraphHeadFactory().getType()));

    // used for type hinting when loading vertex data
    TypeInformation<Tuple1<V>> vertexTypeInfo = new TupleTypeInfo(
      Tuple1.class,
      TypeExtractor.createTypeInfo(config.getVertexFactory().getType()));

    // used for type hinting when loading edge data
    TypeInformation<Tuple1<E>> edgeTypeInfo = new TupleTypeInfo(
      Tuple1.class,
      TypeExtractor.createTypeInfo(config.getEdgeFactory().getType()));


    DataSet<Tuple1<G>> graphHeads = config.getExecutionEnvironment()
      .createInput(
        new GraphHeadTableInputFormat<>(
          config.getGraphHeadHandler(), store.getGraphHeadName()),
        graphTypeInfo);

    DataSet<Tuple1<V>> vertices = config.getExecutionEnvironment()
      .createInput(new VertexTableInputFormat<>(
          config.getVertexHandler(), store.getVertexTableName()),
        vertexTypeInfo);

    DataSet<Tuple1<E>> edges = config.getExecutionEnvironment().createInput(
      new EdgeTableInputFormat<>(
        config.getEdgeHandler(), store.getEdgeTableName()),
      edgeTypeInfo);

    return GraphCollection.fromDataSets(
      graphHeads.map(new ValueOf1<G>()),
      vertices.map(new ValueOf1<V>()),
      edges.map(new ValueOf1<E>()),
      config);
  }

  @Override
  public GraphTransactions<G, V, E> getGraphTransactions() {
    return getGraphCollection().toTransactions();
  }
}
