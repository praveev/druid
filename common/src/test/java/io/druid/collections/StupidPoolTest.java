package io.druid.collections;

import com.google.common.base.Supplier;
import com.metamx.common.ISE;

import org.easymock.EasyMock;
import org.hamcrest.core.IsInstanceOf;
import org.hamcrest.core.IsNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class StupidPoolTest
{
  private Supplier<String> generator;
  private StupidPool<String> poolOfString;
  private ResourceHolder<String> resourceHolderObj;
  private String defaultString = new String("test");

  @Before
  public void setUp()
  {
    generator = EasyMock.createMock(Supplier.class);
    EasyMock.expect(generator.get()).andReturn(defaultString).anyTimes();
    EasyMock.replay(generator);
    poolOfString = new StupidPool<>(generator);
    resourceHolderObj = poolOfString.take();
  }

  @After
  public void tearDown() throws IOException
  {
    if (resourceHolderObj != null) {
      resourceHolderObj.close();
    }
  }

  @Test
  public void testTake()
  {
    Assert.assertThat(resourceHolderObj, new IsInstanceOf(ResourceHolder.class));
    Object expectedObject = resourceHolderObj.get();
    Assert.assertEquals(expectedObject, defaultString);
  }

  @Test(expected = ISE.class)
  public void testExceptionInResourceHolderGet() throws IOException
  {
    resourceHolderObj.close();
    resourceHolderObj.get();
  }

  @Test
  public void testFinalizeInResourceHolder()
  {
    resourceHolderObj = null;
    System.runFinalization();
  }
}