package io.druid.collections;

import com.google.common.collect.ImmutableMap;

import org.junit.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

public class CountingMapTest
{
  private CountingMap mapObject = null ;

  @Before public void setUp() throws Exception
  {
    mapObject = new CountingMap();
  }

  @After public void tearDown() throws Exception
  {
    mapObject.clear();
  }

  @Test public void testAdd() throws Exception
  {
    long defaultValue = 10;
    String defaultKey = "defaultKey";
    long actual;
    actual = mapObject.add(defaultKey,defaultValue);
    Assert.assertEquals("Values does not match", actual, defaultValue);
  }

  @Test public void testSnapshot() throws Exception
  {
    long defaultValue = 10;
    String defaultKey = "defaultKey";
    mapObject.add(defaultKey, defaultValue);
    ImmutableMap snapShotMap = (ImmutableMap) mapObject.snapshot();
    Assert.assertEquals("Maps size does not match",mapObject.size(),snapShotMap.size());
    //AtomicLong expected = new AtomicLong((Long) snapShotMap.get(defaultKey));
    long expected = (long) snapShotMap.get(defaultKey);
    AtomicLong actual = (AtomicLong) mapObject.get(defaultKey);
    Assert.assertEquals("Values for key = "+ defaultKey +" does not match",actual.longValue(),expected);
  }
}