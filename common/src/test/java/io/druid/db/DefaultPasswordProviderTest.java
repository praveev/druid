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
  public void test_from_string_password() {
    DefaultPasswordProvider pp = new DefaultPasswordProvider(pwd);
    Assert.assertEquals(pwd, pp.getPassword());
  }
  
  @Test
  public void test_from_map_password() {
    Map<String,String> m = new HashMap<String,String>();
    m.put(DefaultPasswordProvider.KEY_PASSWORD, pwd);
    DefaultPasswordProvider pp = new DefaultPasswordProvider(m);
    Assert.assertEquals(pwd, pp.getPassword());
  }

  @Test(expected=ISE.class)
  public void test_illegal_password() {
    new DefaultPasswordProvider(new Object());
  }
}
