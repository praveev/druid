package io.druid.cli.validate;

import io.airlift.command.Cli;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

public class DruidJsonValidatorTest
{
  private File inputFile;
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws IOException
  {
    inputFile = temporaryFolder.newFile();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testExceptionCase()
  {
    String type = "";
    Cli<?> parser = Cli.builder("validator")
        .withCommand(DruidJsonValidator.class)
        .build();
    Object command = parser.parse("validator","-f", inputFile.getAbsolutePath(), "-t", type);
    Assert.assertNotNull(command);
    DruidJsonValidator druidJsonValidator = (DruidJsonValidator) command;
    druidJsonValidator.run();
  }

  @Test(expected = RuntimeException.class)
  public void testExceptionCaseNoFile()
  {
    String type = "query";
    Cli<?> parser = Cli.builder("validator")
        .withCommand(DruidJsonValidator.class)
        .build();
    Object command = parser.parse("validator","-f", "", "-t", type);
    Assert.assertNotNull(command);
    DruidJsonValidator druidJsonValidator = (DruidJsonValidator) command;
    druidJsonValidator.run();
  }

  @After public void tearDown()
  {
    temporaryFolder.delete();
  }
}