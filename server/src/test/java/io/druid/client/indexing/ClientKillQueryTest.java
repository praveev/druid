package io.druid.client.indexing;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ClientKillQueryTest
{
  private static final String DATA_SOURCE = "data_source";
  private static final Interval INTERVAL = new Interval(new DateTime(), new DateTime().plus(1));
  ClientKillQuery clientKillQuery;

  @Before
  public void setUp()
  {
    clientKillQuery = new ClientKillQuery(DATA_SOURCE, INTERVAL);
  }

  @After
  public void tearDown()
  {
    clientKillQuery = null;
  }

  @Test
  public void testGetType()
  {
    Assert.assertEquals("kill", clientKillQuery.getType());
  }

  @Test
  public void testGetDataSource()
  {
    Assert.assertEquals(DATA_SOURCE, clientKillQuery.getDataSource());
  }

  @Test
  public void testGetInterval()
  {
    Assert.assertEquals(INTERVAL, clientKillQuery.getInterval());
  }
}