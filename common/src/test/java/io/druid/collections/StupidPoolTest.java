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


public class StupidPoolTest
{
  private Supplier<String> generator;
  private StupidPool<String> poolOfString;
  private ResourceHolder<String> resourceHolderObj;

  @Before
  public void setUp() throws Exception
  {
    generator = EasyMock.createMock(Supplier.class);
    poolOfString = new StupidPool<>(generator);
    resourceHolderObj = poolOfString.take();
  }

  @After
  public void tearDown() throws Exception
  {
    if (resourceHolderObj != null) {
      resourceHolderObj.close();
    }
  }

  @Test
  public void testTake() throws Exception
  {
    Assert.assertThat(resourceHolderObj, new IsInstanceOf(ResourceHolder.class));
    Object nullObject = resourceHolderObj.get();
    Assert.assertThat(nullObject, new IsNull());
  }

  @Test(expected = ISE.class)
  public void testExceptionInResourceHolderGet() throws Exception
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