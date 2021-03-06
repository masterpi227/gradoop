package org.gradoop.model.impl.operators.transformation;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.GradoopTestUtils;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.functions.epgm.Id;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.List;

public class ApplyTransformationTest extends TransformationTest {

  @Test(expected = IllegalArgumentException.class)
  public void testMissingFunctions() {
    new ApplyTransformation<>(null, null, null);
  }

  @Test
  public void testIdEquality() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(TEST_GRAPH);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> inputCollection =
      loader.getGraphCollectionByVariables("g0", "g1");

    List<GradoopId> expectedGraphHeadIds  = Lists.newArrayList();
    List<GradoopId> expectedVertexIds     = Lists.newArrayList();
    List<GradoopId> expectedEdgeIds       = Lists.newArrayList();

    inputCollection.getGraphHeads().map(new Id<GraphHeadPojo>()).output(
      new LocalCollectionOutputFormat<>(expectedGraphHeadIds));
    inputCollection.getVertices().map(new Id<VertexPojo>()).output(
      new LocalCollectionOutputFormat<>(expectedVertexIds));
    inputCollection.getEdges().map(new Id<EdgePojo>()).output(
      new LocalCollectionOutputFormat<>(expectedEdgeIds));

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> outputCollection =
      inputCollection.apply(new ApplyTransformation<>(
        new GraphHeadModifier<GraphHeadPojo>(),
        new VertexModifier<VertexPojo>(),
        new EdgeModifier<EdgePojo>()));

    List<GradoopId> resultGraphHeadIds = Lists.newArrayList();
    List<GradoopId> resultVertexIds    = Lists.newArrayList();
    List<GradoopId> resultEdgeIds      = Lists.newArrayList();

    outputCollection.getGraphHeads().map(new Id<GraphHeadPojo>()).output(
      new LocalCollectionOutputFormat<>(resultGraphHeadIds));
    outputCollection.getVertices().map(new Id<VertexPojo>()).output(
      new LocalCollectionOutputFormat<>(resultVertexIds));
    outputCollection.getEdges().map(new Id<EdgePojo>()).output(
      new LocalCollectionOutputFormat<>(resultEdgeIds));

    getExecutionEnvironment().execute();

    GradoopTestUtils.validateIdEquality(
      expectedGraphHeadIds,
      resultGraphHeadIds);
    GradoopTestUtils.validateIdEquality(
      expectedVertexIds,
      resultVertexIds);
    GradoopTestUtils.validateIdEquality(
      expectedEdgeIds,
      resultEdgeIds);
  }

  @Test
  public void testDataEquality() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(TEST_GRAPH);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> inputCollection =
      loader.getGraphCollectionByVariables("g0", "g1");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> expectedCollection =
      loader.getGraphCollectionByVariables("g01", "g11");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> outputCollection =
      inputCollection.apply(new ApplyTransformation<>(
        new GraphHeadModifier<GraphHeadPojo>(),
        new VertexModifier<VertexPojo>(),
        new EdgeModifier<EdgePojo>()));

    collectAndAssertTrue(
      outputCollection.equalsByGraphData(expectedCollection));
  }

  @Test
  public void testGraphHeadOnlyTransformation() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(TEST_GRAPH);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> inputCollection =
      loader.getGraphCollectionByVariables("g0", "g1");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> expectedCollection =
      loader.getGraphCollectionByVariables("g02", "g12");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> outputCollection =
      inputCollection.apply(
        new ApplyTransformation<GraphHeadPojo, VertexPojo, EdgePojo>(
          new GraphHeadModifier<GraphHeadPojo>(), null, null));

    collectAndAssertTrue(
      outputCollection.equalsByGraphData(expectedCollection));
  }

  @Test
  public void testVertexOnlyTransformation() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(TEST_GRAPH);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> inputCollection =
      loader.getGraphCollectionByVariables("g0", "g1");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> expectedCollection =
      loader.getGraphCollectionByVariables("g03", "g13");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> outputCollection =
      inputCollection.apply(
        new ApplyTransformation<GraphHeadPojo, VertexPojo, EdgePojo>(
          null, new VertexModifier<VertexPojo>(), null));

    collectAndAssertTrue(
      outputCollection.equalsByGraphData(expectedCollection));
  }

  @Test
  public void testEdgeOnlyTransformation() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(TEST_GRAPH);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> inputCollection =
      loader.getGraphCollectionByVariables("g0", "g1");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> expectedCollection =
      loader.getGraphCollectionByVariables("g04", "g14");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> outputCollection =
      inputCollection.apply(
        new ApplyTransformation<GraphHeadPojo, VertexPojo, EdgePojo>(
          null, null, new EdgeModifier<EdgePojo>()));

    collectAndAssertTrue(
      outputCollection.equalsByGraphData(expectedCollection));
  }
}
