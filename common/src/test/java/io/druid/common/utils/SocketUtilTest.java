package io.druid.common.utils;

import org.junit.Assert;
import org.junit.Test;

import org.hamcrest.number.OrderingComparison;
import com.metamx.common.ISE;

public class SocketUtilTest
{
  private final int MAX_PORT = 0xffff;
  @Test
  public void testSocketUtil()
  {
    int port = SocketUtil.findOpenPort(0);
    Assert.assertThat("Port is greater than the maximum port 0xffff",port, OrderingComparison.lessThanOrEqualTo(MAX_PORT));
    Assert.assertThat("Port is less than minimum port 0",port, OrderingComparison.greaterThanOrEqualTo(0));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIllegalArgument()
  {
    SocketUtil.findOpenPort(-1);
  }

  @Test(expected = ISE.class)
  public void testISEexception()
  {
    SocketUtil.findOpenPort(0xffff);
  }
}
