package io.druid.indexer;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import com.google.common.io.ByteStreams;
import com.metamx.common.ISE;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobContext;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class UtilsTest
{
  static final String dummyString = "Very important string";
  final String UTF8 = "UTF-8";
  final String TMP_FILE_NAME = "test_file";
  Configuration jobConfig;
  JobContext mockJobContext;
  List<String> keys;
  Map expectedMap;
  File tmpFile;
  Path tmpPath;
  FileSystem defaultFileSystem;
  Iterable setOfValues;
  Set setOfKeys;

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  private class CreateValueFromKey implements Function
  {
    @Override public Object apply(Object input)
    {
      return input.toString() + UtilsTest.dummyString;
    }
  }

  @Before
  public void setUp() throws IOException
  {
    jobConfig = new Configuration();
    mockJobContext = EasyMock.createMock(JobContext.class);
    EasyMock.expect(mockJobContext.getConfiguration()).andReturn(jobConfig).anyTimes();
    EasyMock.replay(mockJobContext);

    setOfKeys = new HashSet();
    setOfKeys.addAll(new ArrayList<>(Arrays.asList("key1","key2","key3")));
    setOfValues = Iterables.transform(setOfKeys,new CreateValueFromKey());
    expectedMap = (Map<String, Object>) Maps.asMap(setOfKeys, new CreateValueFromKey());

    tmpFile = tmpFolder.newFile(TMP_FILE_NAME);
    tmpPath = new Path(tmpFile.getAbsolutePath());
    defaultFileSystem = tmpPath.getFileSystem(jobConfig);
  }

  @After
  public void tearDown()
  {
    tmpFolder.delete();
  }

  @Test
  public void testZipMap()
  {
    Map actualMap = Utils.zipMap(setOfKeys,setOfValues);
    Assert.assertThat("Hash content is not matching", expectedMap,Is.is(actualMap));
  }

  @Test(expected = ISE.class)
  public void testZipMapValuesMoreThanKeys()
  {
    setOfValues = Iterables.concat(setOfValues,Arrays.asList("more values"));
    Utils.zipMap(setOfKeys, setOfValues);
  }

  @Test
  public void testExistsPlainFile() throws IOException
  {
    boolean expected = Utils.exists(mockJobContext,defaultFileSystem,tmpPath);
    Assert.assertTrue("Should be true since file is created",expected);
    tmpFolder.delete();
    expected = Utils.exists(mockJobContext,defaultFileSystem,tmpPath);
    Assert.assertFalse("Should be false since file is deleted",expected);
    EasyMock.verify(mockJobContext);
  }

  @Test
  public void testPlainStoreThenGetStats() throws IOException
  {
    Utils.storeStats(mockJobContext, tmpPath,expectedMap);
    Map actualMap = Utils.getStats(mockJobContext, tmpPath);
    Assert.assertThat(actualMap,Is.is(actualMap));
    EasyMock.verify(mockJobContext);
  }

  @Test(expected = ISE.class)
  public void testExceptionInMakePathAndOutputStream() throws IOException
  {
    boolean overwrite = false;
    Utils.makePathAndOutputStream(mockJobContext,tmpPath,overwrite);
  }

  @Test
  public void testPlainOpenInputStream() throws IOException
  {
    FileUtils.writeStringToFile(tmpFile,dummyString);
    InputStream inStream = Utils.openInputStream(mockJobContext, tmpPath);
    Assert.assertNotNull(inStream);
    String expected = IOUtils.toString(inStream,UTF8);
    Assert.assertEquals(expected, dummyString);
  }
}