package io.druid.db;

import org.junit.Assert;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.metamx.common.ISE;

public class DefaultPasswordProviderTest
{
  private static final String pwd = "nothing";

  @Test
  public void testFromStringPassword() {
    DefaultPasswordProvider pp = new DefaultPasswordProvider(pwd);
    Assert.assertEquals(pwd, pp.getPassword());
  }
  
  @Test
  public void testFromMapPassword() {
    DefaultPasswordProvider pp = new DefaultPasswordProvider(ImmutableMap.of("password", pwd));
    Assert.assertEquals(pwd, pp.getPassword());
  }

  @Test(expected=ISE.class)
  public void testIllegalPassword() {
    new DefaultPasswordProvider(new Object());
  }
}
