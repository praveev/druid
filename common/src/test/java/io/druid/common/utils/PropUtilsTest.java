package io.druid.common.utils;


import org.junit.Assert;
import org.junit.Test;

import com.metamx.common.ISE;

import java.util.Properties;

/**
Testing PropUtils */

public class PropUtilsTest{
	
	
	@Test(expected=ISE.class)
	public void testNotSpecifiedGetProperty(){
		Properties prop=new Properties();
		PropUtils.getProperty(prop,"");
	}
	
	@Test
	public void testGetProperty(){
		Properties prop=new Properties();
		prop.setProperty("key","value");
		Assert.assertEquals("value", PropUtils.getProperty(prop,"key"));
	}
	
	@Test(expected=ISE.class)
	public void testNotSpecifiedGetPropertyAsInt(){
		Properties prop=new Properties();
		PropUtils.getPropertyAsInt(prop,"",null);
	}
	
	@Test
	public void testGetPropertyAsInt(){
		Properties prop=new Properties();
		Integer expcected =new Integer (1);
		Integer result=PropUtils.getPropertyAsInt(prop,"",expcected);
		Assert.assertEquals(expcected, result);
	}
	
	@Test
	public void testParseGetPropertyAsInt(){
		Properties prop=new Properties();
		prop.setProperty("key","1");
		Assert.assertEquals(1, PropUtils.getPropertyAsInt(prop,"key"));
		
	}
	
	@Test(expected=ISE.class) 
	public void testFormatExceptionGetPropertyAsInt(){
		Properties prop=new Properties();
		prop.setProperty("key","1-value");
		PropUtils.getPropertyAsInt(prop,"key",null);
		
	}
}
