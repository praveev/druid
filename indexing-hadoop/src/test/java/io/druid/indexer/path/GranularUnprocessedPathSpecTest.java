package io.druid.indexer.path;

import org.junit.Assert;
import org.junit.Test;

public class GranularUnprocessedPathSpecTest
{
  @Test
  public void testSetGetMaxBuckets()
  {
    GranularUnprocessedPathSpec granularUnprocessedPathSpec = new GranularUnprocessedPathSpec();
    int maxBuckets = 5;
    granularUnprocessedPathSpec.setMaxBuckets(maxBuckets);
    Assert.assertEquals(maxBuckets,granularUnprocessedPathSpec.getMaxBuckets());
  }
}