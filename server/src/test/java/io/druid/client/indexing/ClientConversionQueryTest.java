package io.druid.client.indexing;

import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

public class ClientConversionQueryTest
{
  private ClientConversionQuery clientConversionQuery;
  private static final String DATA_SOURCE = "data_source";
  private static final Interval INTERVAL = new Interval(new DateTime(), new DateTime().plus(1));
  private static final DataSegment DATA_SEGMENT = new DataSegment(DATA_SOURCE, INTERVAL, new DateTime().toString(), null,
      null, null, null, 0, 0);
  
  @Test public void testGetType()
  {
    clientConversionQuery = new ClientConversionQuery(DATA_SEGMENT);
    Assert.assertEquals("version_converter", clientConversionQuery.getType());
  }

  @Test public void testGetDataSource()
  {
    clientConversionQuery = new ClientConversionQuery(DATA_SEGMENT);
    Assert.assertEquals(DATA_SOURCE, clientConversionQuery.getDataSource());

  }

  @Test public void testGetInterval()
  {
    clientConversionQuery = new ClientConversionQuery(DATA_SEGMENT);
    Assert.assertEquals(INTERVAL, clientConversionQuery.getInterval());
  }

  @Test public void testGetSegment()
  {
    clientConversionQuery = new ClientConversionQuery(DATA_SEGMENT);
    Assert.assertEquals(DATA_SEGMENT, clientConversionQuery.getSegment());
    clientConversionQuery = new ClientConversionQuery(DATA_SOURCE,INTERVAL);
    Assert.assertNull(clientConversionQuery.getSegment());
  }
}