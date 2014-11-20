package io.druid.collections;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class StupidResourceHolderTest
{
  private StupidResourceHolder<String> resourceHolder;

  @Test
  public void testCreateAndGet() throws IOException
  {
    String expected = "String";
    resourceHolder = StupidResourceHolder.create(expected);
    String actual = resourceHolder.get();
    Assert.assertEquals(expected,actual);
    resourceHolder.close();
  }
}