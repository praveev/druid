package io.druid.indexing.common;

import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import org.joda.time.Duration;

public class RetryPolicyFactoryTest
{
  @Test
  public void testMakeRetryPolicy()
  {
    RetryPolicyConfig config = new RetryPolicyConfig()
        .setMinWait(new Period("PT1S"))
        .setMaxWait(new Period("PT10S"))
        .setMaxRetryCount(1);
    RetryPolicyFactory retryPolicyFactory = new RetryPolicyFactory(config);
    RetryPolicy retryPolicy = retryPolicyFactory.makeRetryPolicy();
    Assert.assertEquals(new Duration("PT1S"),retryPolicy.getAndIncrementRetryDelay());
    Assert.assertTrue(retryPolicy.hasExceededRetryThreshold());
  }
}