package org.gradoop.model.impl.operators.limit;

import org.apache.flink.api.common.InvalidProgramException;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.gradoop.util.GradoopFlinkConfig;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LimitTest extends GradoopFlinkTestBase {

  @Test
  public void testInBound() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo>
      loader = getSocialNetworkLoader();

    int limit = 2;

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> inputCollection =
      loader.getGraphCollectionByVariables("g0", "g1", "g2", "g3");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> outputCollection =
      inputCollection.limit(limit);

    assertEquals(limit, outputCollection.getGraphHeads().count());
  }

  @Test
  public void testOutOfBound() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo>
      loader = getSocialNetworkLoader();

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> inputCollection =
      loader.getGraphCollectionByVariables("g0", "g1");

    int limit = 4;
    int expectedLimit = 2;

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      outputCollection = inputCollection.limit(limit);

    assertEquals(expectedLimit, outputCollection.getGraphHeads().count());
  }

  @Test
  public void testEmpty() throws Exception {
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> inputCollection =
      GraphCollection.createEmptyCollection(
        GradoopFlinkConfig.createDefaultConfig(getExecutionEnvironment()));

    int limit = 4;
    int expectedCount = 0;

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      outputCollection = inputCollection.limit(limit);

    assertEquals(expectedCount, outputCollection.getGraphHeads().count());
  }

  @Test(expected = InvalidProgramException.class)
  public void testNegativeLimit() throws Exception {
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> inputCollection =
      GraphCollection.createEmptyCollection(
        GradoopFlinkConfig.createDefaultConfig(getExecutionEnvironment()));

    int limit = -1;
    int expectedCount = 0;

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      outputCollection = inputCollection.limit(limit);

    assertEquals(expectedCount, outputCollection.getGraphHeads().count());
  }
}
