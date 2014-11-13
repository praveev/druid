package io.druid.common.utils;


import org.junit.Assert;
import org.junit.Test;

import com.metamx.common.ISE;

public class SocketUtilTest{
  
  @Test
  public void testSocketUtil() {
    try{
      Assert.assertTrue((SocketUtil.findOpenPort(0)<0xffff));
    }catch(ISE expected){
      Assert.assertTrue(true);
    }
  }
  
  @Test(expected=IllegalArgumentException.class)
  public void testIllegalArgument(){
    SocketUtil.findOpenPort(-1);
  }
  
  @Test(expected=ISE.class)
  public void testISEexception(){
     SocketUtil.findOpenPort(0xffff);
     }
}
 