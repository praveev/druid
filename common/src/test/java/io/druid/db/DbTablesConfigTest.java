package io.druid.db;

import org.junit.Assert;
import org.junit.Test;

public class DbTablesConfigTest
{
  private static final String BASE = "base";
  private static final String SEGMENTS_TABLES = "SEGMENTS_TABLES";
  private static final String RULES_TABLE = "RULES_TABLE";
  private static final String CONFIG_TABLE = "CONFIG_TABLE";
  private static final String TASKS_TABLE = "TASKS_TABLE";
  private static final String TASK_LOG_TABLE = "TASK_LOG_TABLE";
  private static final String TASK_LOCK_TABLE = "TASK_LOCK_TABLE";
  private DbTablesConfig dbTablesConfig = new DbTablesConfig(
      BASE,
      SEGMENTS_TABLES,
      RULES_TABLE,
      CONFIG_TABLE,
      TASKS_TABLE,
      TASK_LOG_TABLE,
      TASK_LOCK_TABLE
      );

  @Test
  public void testFromBase()
  {
    Assert.assertEquals(dbTablesConfig.getBase(), DbTablesConfig.fromBase(BASE).getBase());
  }

  @Test
  public void testGetSegmentsTable()
  {
    Assert.assertEquals(SEGMENTS_TABLES, dbTablesConfig.getSegmentsTable());
  }

  @Test
  public void testGetRulesTable()
  {
    Assert.assertEquals(RULES_TABLE, dbTablesConfig.getRulesTable());
  }

  @Test
  public void testGetConfigTable()
  {
    Assert.assertEquals(CONFIG_TABLE, dbTablesConfig.getConfigTable());
  }

  @Test
  public void testGetTasksTable()
  {
    Assert.assertEquals(TASKS_TABLE, dbTablesConfig.getTasksTable());
  }

  @Test
  public void testGetTaskLogTable()
  {
    Assert.assertEquals(TASK_LOG_TABLE, dbTablesConfig.getTaskLogTable());
  }

  @Test
  public void testGetTaskLockTable()
  {
    Assert.assertEquals(TASK_LOCK_TABLE, dbTablesConfig.getTaskLockTable());
  }
}