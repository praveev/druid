package io.druid.indexer.path;

import com.metamx.common.Granularity;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class GranularityPathSpecTest
{
  private GranularityPathSpec granularityPathSpec;
  private final String TEST_STRING_PATH = "TEST";
  private final String TEST_STRING_PATTERN = "*.TEST";
  private final String TEST_STRING_FORMAT = "F_TEST";

  @Before public void setUp()
  {
    granularityPathSpec = new GranularityPathSpec();
  }

  @After public void tearDown()
  {
    granularityPathSpec = null;
  }

  @Test public void testSetInputPath()
  {
    granularityPathSpec.setInputPath(TEST_STRING_PATH);
    Assert.assertEquals(TEST_STRING_PATH,granularityPathSpec.getInputPath());
  }

  @Test public void testSetFilePattern()
  {
    granularityPathSpec.setFilePattern(TEST_STRING_PATTERN);
    Assert.assertEquals(TEST_STRING_PATTERN,granularityPathSpec.getFilePattern());
  }

  @Test public void testSetPathFormat()
  {
    granularityPathSpec.setPathFormat(TEST_STRING_FORMAT);
    Assert.assertEquals(TEST_STRING_FORMAT,granularityPathSpec.getPathFormat());
  }

  @Test public void testSetDataGranularity()
  {
    Granularity granularity = Granularity.DAY;
    granularityPathSpec.setDataGranularity(granularity);
    Assert.assertEquals(granularity,granularityPathSpec.getDataGranularity());
  }
}