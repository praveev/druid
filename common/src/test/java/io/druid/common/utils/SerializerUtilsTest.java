package io.druid.common.utils;


import org.junit.Assert;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;

import io.druid.collections.IntList;


public class SerializerUtilsTest{
	
	private SerializerUtils serializerUtils;
	private  final float delta=0;
	private  final String [] strings ={"1#","2","3"};
	private  final int [] ints={1,2,3};
	private  final float [] floats={1.1f,2,3};
	private  final long [] longs={3,2,1};
	private  final Charset UTF8 = Charset.forName("UTF-8");
	
	private  byte [] stringsByte;
	private  byte [] intsByte;
	private  byte [] floatsByte;
	private  byte [] longsByte;
	
	private ByteArrayOutputStream outStream;
	
	@Before
	public  void  setUpByteArrays() throws IOException{
		//write the inputs to a byte array 
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
	    DataOutputStream out = new DataOutputStream(bos);
		//strings
		out.writeInt(strings.length);//write the length 
		for(int i=0;i<strings.length;i++){//write each string by length + string
			byte[] stringBytes = strings[i].getBytes(UTF8);
			out.writeInt(stringBytes.length);
			out.write(strings[i].getBytes());
		}
		out.close();
	    stringsByte = bos.toByteArray();
	    bos.close();
		//Ints 
	    bos = new ByteArrayOutputStream();
	    out = new DataOutputStream(bos);
	    out.writeInt(ints.length);//write the length
	    for(int i=0;i<ints.length;i++){
	    	out.writeInt(ints[i]); //write numbers
	    }
	    out.close();
	    intsByte = bos.toByteArray();
	    bos.close();
	    
		//floats same as int
		
	   bos = new ByteArrayOutputStream();
	   out = new DataOutputStream(bos);
	   out.writeInt(floats.length);//write the length
	    
	    for(int i=0;i<ints.length;i++){
	    	out.writeFloat(floats[i]); //write numbers
	    }
	    out.close();
	    floatsByte = bos.toByteArray();
	    bos.close();
	    
		//longs same 
	    bos = new ByteArrayOutputStream();
		out = new DataOutputStream(bos);
		out.writeInt(longs.length);//write the length
		for(int i=0;i<longs.length;i++){
		    out.writeLong(longs[i]); //write numbers
		    }
		out.close();
		longsByte = bos.toByteArray();
		bos.close();
		
	}
	
	@Before
	public void setUp() throws IOException{
		serializerUtils= new SerializerUtils();
		outStream= new ByteArrayOutputStream();
	}
	
	@Test
	public void testWriteInts() throws IOException{
		
		
		//call the target
		serializerUtils.writeInts(outStream, ints);
		//read back
		byte [] actuals= outStream.toByteArray();
		//assert
		Assert.assertArrayEquals(intsByte, actuals);
		
	}
	
	@Test
	public void testWriteIntList() throws IOException{
		
		IntList list =new IntList();
		for(int i=0;i<ints.length;i++){
			list.add(ints[i]);
		}
		serializerUtils.writeInts(outStream, list);
		//read back
		byte [] actuals= outStream.toByteArray();
		//assert
		Assert.assertArrayEquals(intsByte, actuals);
		
	}
	
	@Test
	public void testWriteFloats() throws IOException{
		//call the target
		serializerUtils.writeFloats(outStream, floats);
		//read back
		byte [] actuals= outStream.toByteArray();
		//assert
		Assert.assertArrayEquals(floatsByte, actuals);
		
	}
	
	@Test
	public void testChannelWritefloat() throws IOException {
		final int index=0; 
		WritableByteChannel channelOutput = Channels.newChannel(outStream);
		serializerUtils.writeFloat(channelOutput, floats[index]);
		ByteArrayInputStream inputstream = new ByteArrayInputStream(outStream.toByteArray());
		if(channelOutput!=null) channelOutput.close();
		float expected=serializerUtils.readFloat(inputstream);
		float actuals=floats[index];
		Assert.assertEquals(expected, actuals,delta);
		
	}
	
	@Test
	public void testWriteLongs() throws IOException{
		//call the target
		serializerUtils.writeLongs(outStream, longs);
		//read back
		byte [] actuals= outStream.toByteArray();
		//assert
		Assert.assertArrayEquals(longsByte,actuals);
		
	}
	
	
	@Test
	public void testWriteStrings() throws IOException{
		//call the target
		serializerUtils.writeStrings(outStream, strings);
		//read back
		byte [] actuals= outStream.toByteArray();
		//assert
		Assert.assertArrayEquals(stringsByte,actuals);
		

	}
	
	@Test
	public void testChannelWritelong() throws IOException {
		final int index=0; 
		WritableByteChannel channelOutput = Channels.newChannel(outStream);
		serializerUtils.writeLong(channelOutput, longs[index]);
		ByteArrayInputStream inputstream = new ByteArrayInputStream(outStream.toByteArray());
		channelOutput.close();
		inputstream.close();
		long expected=serializerUtils.readLong(inputstream);
		long actuals=longs[index];
		Assert.assertEquals(expected, actuals);
		
	}
	
	@Test
	public void testReadInts() throws IOException{
		ByteArrayInputStream inputstream= new ByteArrayInputStream(intsByte);
		//call the second target
		int  [] actuals=serializerUtils.readInts(inputstream);
		inputstream.close();
		Assert.assertArrayEquals(ints, actuals);
	}
	
	@Test
	public void testReadFloats() throws IOException{
		ByteArrayInputStream inputstream= new ByteArrayInputStream(floatsByte);
		//call the second target
		float  [] actuals=serializerUtils.readFloats(inputstream);
		inputstream.close();
		Assert.assertArrayEquals(floats, actuals, delta);
	}
	
	
	@Test
	public void testReadLongs() throws IOException{
		ByteArrayInputStream inputstream= new ByteArrayInputStream(longsByte);
		//call the second target
		long  [] actuals=serializerUtils.readLongs(inputstream);
		inputstream.close();
		Assert.assertArrayEquals(longs, actuals);
	}
	
	
	
	@Test 
	public void testReadStrings()throws IOException{
		ByteArrayInputStream inputstream= new ByteArrayInputStream(stringsByte);
		//call the second target
		String  [] actuals=serializerUtils.readStrings(inputstream);
		inputstream.close();
		Assert.assertArrayEquals(strings, actuals);
	}
	
	
	@Test
	public void testChannelWriteString() throws IOException {
		final int index=0; 
		WritableByteChannel channelOutput = Channels.newChannel(outStream);
		serializerUtils.writeString(channelOutput, strings[index]);
		ByteArrayInputStream inputstream = new ByteArrayInputStream(outStream.toByteArray());
		channelOutput.close();
		inputstream.close();
		String expected=serializerUtils.readString(inputstream);
		String actuals=strings[index];
		Assert.assertEquals(expected, actuals);
		
	}
	
	@Test 
	public void testByteBufferReadStrings() throws IOException{
		//call the second target
		ByteBuffer buffer=ByteBuffer.allocate(stringsByte.length);
		buffer.put(stringsByte);
		buffer.flip();
		String  [] actuals=serializerUtils.readStrings(buffer);
		Assert.assertArrayEquals(strings, actuals);
	}
		
	@After
	public void tearDown() throws IOException{
		serializerUtils=null;
		outStream.close();
	}
	
}