package io.druid.collections;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class StupidResourceHolderTest
{
  private StupidResourceHolder<String> resourceHolder;

  @After public void tearDown() throws Exception
  {
    resourceHolder.close();
  }

  @Test public void testCreateAndGet() throws Exception
  {
    String expected = "String";
    resourceHolder = StupidResourceHolder.create(expected);
    String actual = resourceHolder.get();
    Assert.assertEquals(expected,actual);
  }

}