package org.gradoop.model.impl.properties;

import org.gradoop.GradoopTestUtils;
import org.gradoop.model.GradoopFlinkTestBase;
import org.junit.Test;

import static org.gradoop.model.impl.GradoopFlinkTestUtils.writeAndRead;
import static org.junit.Assert.assertEquals;

public class PropertiesSerializationTest extends GradoopFlinkTestBase {

  @Test
  public void testPropertyValueSerialization() throws Exception {
    PropertyValue pIn = PropertyValue.create(10L);
    assertEquals("Property Values were not equal", pIn,
      writeAndRead(pIn, getExecutionEnvironment()));
  }

  @Test
  public void testPropertySerialization() throws Exception {
    Property pIn = Property.create("key1", 10L);
    assertEquals("Properties were not equal", pIn,
      writeAndRead(pIn, getExecutionEnvironment()));
  }

  @Test
  public void testPropertyListSerialization() throws Exception {
    PropertyList pIn = PropertyList.createFromMap(
      GradoopTestUtils.SUPPORTED_PROPERTIES);
    assertEquals("Property Lists were not equal", pIn,
      writeAndRead(pIn, getExecutionEnvironment()));
  }
}
