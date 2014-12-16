package io.druid.client.indexing;

import com.google.common.collect.Lists;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class ClientAppendQueryTest
{
  private ClientAppendQuery clientAppendQuery;
  private static final String DATA_SOURCE = "data_source";
  private List<DataSegment> segments = Lists.<DataSegment>newArrayList(
      new DataSegment(DATA_SOURCE, new Interval(new DateTime(), new DateTime().plus(1)), new DateTime().toString(), null,
          null, null, null, 0, 0));
  @Before
  public void setUp()
  {
    clientAppendQuery = new ClientAppendQuery(DATA_SOURCE, segments);
  }

  @After
  public void tearDown()
  {
    clientAppendQuery = null;
  }

  @Test
  public void testGetType()
  {
    Assert.assertEquals("append",clientAppendQuery.getType());

  }

  @Test
  public void testGetDataSource()
  {
    Assert.assertEquals(DATA_SOURCE, clientAppendQuery.getDataSource());
  }

  @Test
  public void testGetSegments()
  {
    Assert.assertEquals(segments, clientAppendQuery.getSegments());
  }

  @Test
  public void testToString()
  {
    Assert.assertTrue(clientAppendQuery.toString().contains(DATA_SOURCE));
    Assert.assertTrue(clientAppendQuery.toString().contains(segments.toString()));
  }
}