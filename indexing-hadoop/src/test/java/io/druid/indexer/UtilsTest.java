package io.druid.indexer;

import com.metamx.common.ISE;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.mapred.JobContext;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.easymock.EasyMock;
import org.hamcrest.core.Is;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UtilsTest
{
  final int TEST_MAP_SIZE = 4;
  final String ITEM = "Very important string";
  final Charset UTF8 = Charset.forName("UTF-8");
  static final String TMP_FILE_NAME = "test_file";
  static final Class<? extends CompressionCodec> DEFAULT_COMPRESSION_CODEC = GzipCodec.class;
  static final String CODEC_CLASS = "org.apache.hadoop.io.compress.GzipCodec";
  Configuration jobConfig;
  JobContext mockJobContext;
  List<String> keys;
  List<String> values;
  HashMap<String,Object> expectedMap;

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Before
  public void setUp()
  {
    jobConfig = new Configuration();
    mockJobContext = EasyMock.createMock(JobContext.class);
    EasyMock.expect(mockJobContext.getConfiguration()).andReturn(jobConfig).anyTimes();
    EasyMock.replay(mockJobContext);
    keys = new ArrayList<>();
    values = new ArrayList<>();
    expectedMap = new HashMap<>();
    for(int i = 0;i < TEST_MAP_SIZE;i++) {
      keys.add(Integer.toString(i));
      values.add(ITEM);
      expectedMap.put(Integer.toString(i), new String(ITEM));
    }
  }

  @After
  public void tearDown()
  {
    tmpFolder.delete();
  }

  @Test
  public void testZipMap()
  {
    Map actualMap = Utils.zipMap(keys,values);
    Assert.assertEquals("Size of the hashMap is not matching",keys.size(),actualMap.size());
    Assert.assertThat("Hash content is not matching", expectedMap,Is.is(actualMap));
  }

  @Test(expected = ISE.class)
  public void testZipMapValuesMoreThanKeys()
  {
    values.add(ITEM);
    Utils.zipMap(keys,values);
  }

  @Test
  public void testExistsPlainFile() throws IOException
  {
    File tmpFile = tmpFolder.newFile();
    Path tmpPath = new Path(tmpFile.getAbsolutePath());
    boolean expected = false;
    FileSystem HDfs = tmpPath.getFileSystem(jobConfig);
    expected = Utils.exists(mockJobContext,HDfs,tmpPath);
    Assert.assertTrue("Should be true since file is created",expected);
    tmpFolder.delete();
    expected = Utils.exists(mockJobContext,HDfs,tmpPath);
    Assert.assertFalse("Should be false since file is deleted",expected);
    EasyMock.verify(mockJobContext);
  }

  @Test public void testExistsCompressedFile() throws IOException
  {
    jobConfig.setBoolean(FileOutputFormat.COMPRESS,true);
    jobConfig.set(FileOutputFormat.COMPRESS_CODEC, CODEC_CLASS);
    Class<? extends CompressionCodec> codecClass = FileOutputFormat
        .getOutputCompressorClass(mockJobContext, DEFAULT_COMPRESSION_CODEC);
    CompressionCodec codec = ReflectionUtils.newInstance(codecClass, jobConfig);
    File tmpFile = tmpFolder.newFile(TMP_FILE_NAME + codec.getDefaultExtension());
    Path tmpPath = new Path(tmpFile.getParent() + File.separator + TMP_FILE_NAME);
    boolean expected = false;
    FileSystem HDfs = tmpPath.getFileSystem(jobConfig);
    expected = Utils.exists(mockJobContext,HDfs,tmpPath);
    Assert.assertTrue("Should be true since file is created",expected);
    tmpFolder.delete();
    expected = Utils.exists(mockJobContext,HDfs,tmpPath);
    Assert.assertFalse("Should be false since file is deleted",expected);
    EasyMock.verify(mockJobContext);
  }


  @Test
  public void testPlainOpenInputStream() throws IOException
  {
    File tmpFile = tmpFolder.newFile();
    Path tmpPath = new Path(tmpFile.getPath());
    InputStream inStream = Utils.openInputStream(mockJobContext, tmpPath);
    Assert.assertNotNull(inStream);
    FileUtils.writeStringToFile(tmpFile,ITEM);
    String expected = IOUtils.toString(inStream);
    Assert.assertEquals(expected,ITEM);
    EasyMock.verify(mockJobContext);
  }

  @Test
  public void testCompressedOpenInputStream() throws IOException
  {
    jobConfig.setBoolean(FileOutputFormat.COMPRESS,true);
    jobConfig.set(FileOutputFormat.COMPRESS_CODEC, CODEC_CLASS);
    Class<? extends CompressionCodec> codecClass = FileOutputFormat
        .getOutputCompressorClass(mockJobContext, DEFAULT_COMPRESSION_CODEC);
    CompressionCodec codec = ReflectionUtils.newInstance(codecClass, jobConfig);
    File tmpFile = tmpFolder.newFile(TMP_FILE_NAME + codec.getDefaultExtension());
    Path tmpPathWithOutExtension = new Path(tmpFile.getParent() + File.separator + TMP_FILE_NAME);
    Path tmpPathWithExtension = new Path(tmpFile.getAbsolutePath());
    FileSystem HDfs = tmpPathWithExtension.getFileSystem(jobConfig);
    boolean overwrite = true;
    OutputStream outFileStream = codec.createOutputStream(HDfs.create(tmpPathWithExtension, overwrite));
    outFileStream.write(ITEM.getBytes(UTF8));
    outFileStream.flush();
    outFileStream.close();
    InputStream inStream = Utils.openInputStream(mockJobContext, tmpPathWithOutExtension);
    Assert.assertNotNull("Input stream should not be Null",inStream);
    String actual = IOUtils.toString(inStream);
    Assert.assertEquals("Strings not matching",ITEM,actual);
    EasyMock.verify(mockJobContext);
  }

  @Test
  public void testPlainStoreThenGetStats() throws IOException
  {
    File tmpFile = tmpFolder.newFile();
    Path tmpPath = new Path(tmpFile.getPath());
    Utils.storeStats(mockJobContext, tmpPath,expectedMap);
    Map actualMap = Utils.getStats(mockJobContext, tmpPath);
    Assert.assertThat(actualMap,Is.is(actualMap));
    EasyMock.verify(mockJobContext);
  }

  @Test
  public void testCompressedMakePathAndOutputStream() throws IOException
  {
    jobConfig.setBoolean(FileOutputFormat.COMPRESS,true);
    jobConfig.set(FileOutputFormat.COMPRESS_CODEC, CODEC_CLASS);
    File tmpFile = tmpFolder.newFile(TMP_FILE_NAME);
    Path tmpPathWithoutExtension = new Path(tmpFile.getAbsolutePath());
    boolean overwrite = true;
    OutputStream outStream = Utils.makePathAndOutputStream(mockJobContext,tmpPathWithoutExtension,overwrite);
    Assert.assertNotNull("Output stream should not ne null",outStream);
    IOUtils.write(ITEM.getBytes(), outStream);
    outStream.flush();
    outStream.close();
    Class<? extends CompressionCodec> codecClass = FileOutputFormat
        .getOutputCompressorClass(mockJobContext, DEFAULT_COMPRESSION_CODEC);
    CompressionCodec codec = ReflectionUtils.newInstance(codecClass, jobConfig);
    Path tmpPathWithExtension = new Path(tmpFile.getAbsolutePath() + codec.getDefaultExtension());
    FileSystem fileSystem = tmpPathWithoutExtension.getFileSystem(mockJobContext.getConfiguration());
    InputStream inStream = codec.createInputStream(fileSystem.open(tmpPathWithExtension));
    String actual = IOUtils.toString(inStream);
    Assert.assertEquals("Strings not matching",ITEM,actual);
    EasyMock.verify(mockJobContext);
  }

  @Test(expected = ISE.class)
  public void testExceptionInMakePathAndOutputStream() throws IOException
  {
    File tmpFile = tmpFolder.newFile(TMP_FILE_NAME);
    Path tmpPath = new Path(tmpFile.getAbsolutePath());
    boolean overwrite = false;
    Utils.makePathAndOutputStream(mockJobContext,tmpPath,overwrite);
  }

}