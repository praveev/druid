package io.druid.server.log;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.CharStreams;
import io.druid.server.RequestLogLine;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class FileRequestLoggerTest
{
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
  private static final String HOST = "localhost";

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test public void testLog() throws IOException
  {
    ObjectMapper objectMapper = new ObjectMapper();
    DateTime dateTime = new DateTime();
    File logDir = temporaryFolder.newFolder();
    String actualLogString = new String(dateTime.toString()+"\t"+HOST);

    FileRequestLogger fileRequestLogger = new FileRequestLogger(objectMapper, scheduler, logDir);
    fileRequestLogger.start();
    RequestLogLine requestLogLine = EasyMock.createMock(RequestLogLine.class);
    EasyMock.expect(requestLogLine.getLine((ObjectMapper) EasyMock.anyObject())).
        andReturn(actualLogString).anyTimes();
    EasyMock.replay(requestLogLine);
    fileRequestLogger.log(requestLogLine);
    File logFile = new File(logDir, dateTime.toString("yyyy-MM-dd'.log'"));
    String logString = CharStreams.toString(new FileReader(logFile));
    Assert.assertTrue(logString.contains(actualLogString));
    fileRequestLogger.stop();
  }
}