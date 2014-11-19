package io.druid.common.utils;

import org.junit.Assert;
import org.junit.Test;

import com.metamx.common.ISE;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PropUtilsTest
{
  @Test(expected = ISE.class)
  public void testNotSpecifiedGetProperty()
  {
    Properties prop = new Properties();
    PropUtils.getProperty(prop,"");
  }

  @Test
  public void testGetProperty()
  {
    Properties prop = new Properties();
    prop.setProperty("key","value");
    Assert.assertEquals("value", PropUtils.getProperty(prop,"key"));
  }

  @Test(expected = ISE.class)
  public void testNotSpecifiedGetPropertyAsInt()
  {
    Properties prop = new Properties();
    PropUtils.getPropertyAsInt(prop,"",null);
  }

  @Test
  public void testDefaultValueGetPropertyAsInt()
  {
    Properties prop = new Properties();
    int defaultValue = 1;
    int result = PropUtils.getPropertyAsInt(prop,"",defaultValue);
    Assert.assertEquals(defaultValue, result);
  }

  @Test
  public void testParseGetPropertyAsInt()
  {
    Properties prop = new Properties();
    int expectedValue = 1;
    prop.setProperty("key", Integer.toString(expectedValue));
    int result = PropUtils.getPropertyAsInt(prop,"key");
    Assert.assertEquals(expectedValue, result);
  }

  @Test(expected = ISE.class)
  public void testFormatExceptionGetPropertyAsInt()
  {
    Properties prop = new Properties();
    prop.setProperty("key","1-value");
    PropUtils.getPropertyAsInt(prop,"key",null);
  }

  @Test
  public void testParseStringAsMap_emptyMap()
  {
    Assert.assertEquals(Collections.<String,String>emptyMap(), PropUtils.parseStringAsMap("", ";", ":"));
    Assert.assertEquals(Collections.<String,String>emptyMap(), PropUtils.parseStringAsMap("   ", ";", ":"));
    Assert.assertEquals(Collections.<String,String>emptyMap(), PropUtils.parseStringAsMap(" ;  ; ", ";", ":"));
  }
  
  @Test
  public void testParseStringAsMap_singleElemMap()
  {
    Map<String,String> expected = new HashMap<>();
    expected.put("k1", "v1");

    Assert.assertEquals(expected, PropUtils.parseStringAsMap("k1:v1", ";", ":"));
    Assert.assertEquals(expected, PropUtils.parseStringAsMap("  k1:v1  ", ";", ":"));
    Assert.assertEquals(expected, PropUtils.parseStringAsMap("  k1:v1;  ", ";", ":"));
  }
  
  @Test
  public void testParseStringAs_multiElemMap()
  {
    Map<String,String> expected = new HashMap<>();
    expected.put("k1", "v1");
    expected.put("k2", "v2");

    Assert.assertEquals(expected, PropUtils.parseStringAsMap("k1:v1;k2:v2", ";", ":"));
    Assert.assertEquals(expected, PropUtils.parseStringAsMap("k2:v2;k1:v1", ";", ":"));
    Assert.assertEquals(expected, PropUtils.parseStringAsMap("  k1:v1;k2:v2  ", ";", ":"));
    Assert.assertEquals(expected, PropUtils.parseStringAsMap("  ;  k1:v1; ;k2:v2 ;", ";", ":"));
  }
}
