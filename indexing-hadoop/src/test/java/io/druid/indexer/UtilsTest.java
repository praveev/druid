package io.druid.indexer;

import com.metamx.common.ISE;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import static org.hamcrest.core.Is.is;

import java.util.*;

public class UtilsTest
{
  final int MAPSIZE = 4;
  final String ITEM = "-123-";
  List<Integer> keys;
  List<String> values;
  HashMap<Integer,String> expectedMap;

  @Before
  public void setUp() throws Exception
  {
    keys = new ArrayList<Integer>();
    values = new ArrayList<String>();
    expectedMap = new HashMap<>();
    for(int i = 0;i < MAPSIZE;i++) {
      keys.add(i);
      values.add(ITEM);
      expectedMap.put(i,ITEM);
    }
  }

  @After
  public void tearDown() throws Exception
  {

  }

  @Test
  public void testZipMap() throws Exception
  {
    Map actualMap = Utils.zipMap(keys,values);
    Assert.assertEquals("Size of the hashMap is not matching",keys.size(),actualMap.size());
    Assert.assertThat("Hash content is not matching",expectedMap,is(actualMap));
  }

  @Test(expected = ISE.class)
  public void testZipMapValuesMoreThanKeys(){
    values.add(ITEM);
    Map actualMap = Utils.zipMap(keys,values);
  }

  @Test
  public void testMakePathAndOutputStream() throws Exception
  {

  }

  @Test public void testOpenInputStream() throws Exception
  {

  }

  @Test public void testExists() throws Exception
  {

  }

  @Test public void testOpenInputStream1() throws Exception
  {

  }

  @Test public void testGetStats() throws Exception
  {

  }

  @Test public void testStoreStats() throws Exception
  {

  }
}