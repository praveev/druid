package io.druid.db;

import org.junit.Assert;
import org.junit.Test;
import org.joda.time.Period;

public class DatabaseRuleManagerConfigTest
{
  DatabaseRuleManagerConfig  databaseRuleManagerConfig = new DatabaseRuleManagerConfig();
  @Test
  public void testGetDefaultRule()
  {
    Assert.assertEquals("_default", databaseRuleManagerConfig.getDefaultRule());
  }

  @Test
  public void testGetPollDuration()
  {
    Assert.assertEquals( new Period("PT1M"), databaseRuleManagerConfig.getPollDuration());
  }
}