package io.druid.indexer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.fs.Path;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.easymock.EasyMock;

import java.io.IOException;
public class HadoopIOPeonTest
{
  final String TMP_FILE_NAME = "test_file";
  JobContext mockJobContext;
  Configuration jobConfig;
  boolean overwritesFiles = true;
  HadoopIOPeon IOPeon;

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Before public void setUp() throws IOException
  {
    jobConfig = new Configuration();
    mockJobContext = EasyMock.createMock(JobContext.class);
    EasyMock.expect(mockJobContext.getConfiguration()).andReturn(jobConfig).anyTimes();
    EasyMock.replay(mockJobContext);

    IOPeon = new HadoopIOPeon(mockJobContext,new Path(tmpFolder.newFile().getParent()),overwritesFiles);
  }

  @After public void tearDown()
  {
    jobConfig = null;
    mockJobContext = null;
    tmpFolder.delete();
  }

  @Test public void testMakeOutputStream() throws IOException
  {
    Assert.assertNotNull(IOPeon.makeOutputStream(TMP_FILE_NAME));
  }

  @Test public void testMakeInputStream() throws IOException
  {
    Assert.assertNotNull(IOPeon.makeInputStream(tmpFolder.newFile(TMP_FILE_NAME).getName()));
  }

  @Test(expected = UnsupportedOperationException.class) public void testCleanup() throws IOException
  {
    IOPeon.cleanup();
  }
}