package io.druid.db;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;

import org.junit.Test;

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
    Map<String,String> m = new HashMap<String,String>();
    m.put(DefaultPasswordProvider.PASSWORD_KEY, pwd);
    DefaultPasswordProvider pp = new DefaultPasswordProvider(m);
    Assert.assertEquals(pwd, pp.getPassword());
  }

  @Test(expected=ISE.class)
  public void testIllegalPassword() {
    new DefaultPasswordProvider(new Object());
  }
}
