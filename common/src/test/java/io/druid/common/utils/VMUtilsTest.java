package io.druid.common.utils;

import org.junit.Assert;
import org.junit.Test;

public class VMUtilsTest
{
  @Test
  public void testgetMaxDirectMemory()
  {
    try {
      long maxMemory = VMUtils.getMaxDirectMemory();
      Assert.assertTrue((maxMemory > 0));
    } catch (UnsupportedOperationException expected) {
      Assert.assertTrue(true);
    } catch (RuntimeException expected) {
      Assert.assertTrue(true);
    }
  }
}