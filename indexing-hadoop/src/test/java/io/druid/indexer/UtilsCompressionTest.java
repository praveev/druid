package io.druid.indexer;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.easymock.EasyMock;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class UtilsCompressionTest
{

  static final String dummyString = "Very important string";
  final String UTF8 = "UTF-8";
  final String TMP_FILE_NAME = "test_file";
  final Class<? extends CompressionCodec> DEFAULT_COMPRESSION_CODEC = GzipCodec.class;
  final String CODEC_CLASS = "org.apache.hadoop.io.compress.GzipCodec";
  Configuration jobConfig;
  JobContext mockJobContext;
  FileSystem defaultFileSystem;
  CompressionCodec codec;
  List<String> keys;
  List<String> values;
  Map<String,Object> expectedMap;
  File tmpFile;
  Path tmpPathWithoutExtension;
  Path tmpPathWithExtension;
  Iterable setOfValues;
  Set setOfKeys;

  private class CreateValueFromKey implements Function
  {
    @Override public Object apply(Object input)
    {
      return input.toString() + UtilsCompressionTest.dummyString;
    }
  }

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Before
  public void setUp() throws IOException
  {
    jobConfig = new Configuration();
    mockJobContext = EasyMock.createMock(JobContext.class);
    EasyMock.expect(mockJobContext.getConfiguration()).andReturn(jobConfig).anyTimes();
    EasyMock.replay(mockJobContext);

    jobConfig.setBoolean(FileOutputFormat.COMPRESS, true);
    jobConfig.set(FileOutputFormat.COMPRESS_CODEC, CODEC_CLASS);
    Class<? extends CompressionCodec> codecClass = FileOutputFormat
        .getOutputCompressorClass(mockJobContext, DEFAULT_COMPRESSION_CODEC);
    codec = ReflectionUtils.newInstance(codecClass, jobConfig);

    setOfKeys = new HashSet(new ArrayList<>(Arrays.asList("key1", "key2", "key3")));
    setOfValues = Iterables.transform(setOfKeys, new CreateValueFromKey());
    expectedMap = (Map<String, Object>) Maps.asMap(setOfKeys, new CreateValueFromKey());

    tmpFile = tmpFolder.newFile(TMP_FILE_NAME + codec.getDefaultExtension());
    tmpPathWithExtension = new Path(tmpFile.getAbsolutePath());
    tmpPathWithoutExtension = new Path(tmpFile.getParent() + File.separator + TMP_FILE_NAME);
    defaultFileSystem = tmpPathWithoutExtension.getFileSystem(jobConfig);
  }

  @After
  public void tearDown()
  {
    tmpFolder.delete();
  }

  @Test public void testExistsCompressedFile() throws IOException
  {
    boolean expected = Utils.exists(mockJobContext,defaultFileSystem,tmpPathWithoutExtension);
    Assert.assertTrue("Should be true since file is created", expected);
    tmpFolder.delete();
    expected = Utils.exists(mockJobContext,defaultFileSystem,tmpPathWithoutExtension);
    Assert.assertFalse("Should be false since file is deleted",expected);
  }

  @Test
  public void testCompressedOpenInputStream() throws IOException
  {
    boolean overwrite = true;
    OutputStream outStream = codec.createOutputStream(defaultFileSystem.create(tmpPathWithExtension, overwrite));
    writeStingToOutputStream(dummyString,outStream);
    InputStream inStream = Utils.openInputStream(mockJobContext, tmpPathWithoutExtension);
    Assert.assertNotNull("Input stream should not be Null",inStream);
    String actual = IOUtils.toString(inStream, UTF8);
    Assert.assertEquals("Strings not matching",dummyString,actual);
    inStream.close();
  }

  @Test
  public void testCompressedMakePathAndOutputStream() throws IOException
  {
    boolean overwrite = true;
    OutputStream outStream = Utils.makePathAndOutputStream(mockJobContext,tmpPathWithoutExtension, overwrite);
    Assert.assertNotNull("Output stream should not be null",outStream);
    writeStingToOutputStream(dummyString,outStream);
    InputStream inStream = codec.createInputStream(defaultFileSystem.open(tmpPathWithExtension));
    String actual = IOUtils.toString(inStream,UTF8);
    Assert.assertEquals("Strings not matching",dummyString,actual);
    inStream.close();
  }

  private void writeStingToOutputStream(String string, OutputStream outStream) throws IOException
  {
    IOUtils.write(string.getBytes(UTF8), outStream);
    outStream.flush();
    outStream.close();
  }
}