package io.druid.db;

import io.druid.jackson.JsonTestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.joda.time.Period;

public class DatabaseRuleManagerConfigTest
{
  private final static String defaultRule = "_default";
  private final static Period defaultPeriod = new Period("PT1M");
  DatabaseRuleManagerConfig databaseRuleManagerConfig = new DatabaseRuleManagerConfig();
  @Test
  public void testSerializationDeSerialization() throws Exception
  {
    Assert.assertNotNull(JsonTestUtils.jsonWriteReadWrite(databaseRuleManagerConfig));
  }

  @Test
  public void testGetDefaultRule()
  {
    Assert.assertEquals(defaultRule, databaseRuleManagerConfig.getDefaultRule());
  }

  @Test
  public void testGetPollDuration()
  {
    Assert.assertEquals(defaultPeriod, databaseRuleManagerConfig.getPollDuration());
  }
}