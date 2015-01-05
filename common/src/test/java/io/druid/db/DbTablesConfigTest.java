package io.druid.db;

import org.junit.Assert;
import org.junit.Test;

public class DbTablesConfigTest
{
  private static final String SEGMENTS_TABLES = "_segments";
  private static final String RULES_TABLE = "_rules";
  private static final String CONFIG_TABLE = "_config";
  private static final String TASKS_TABLE = "_tasks";
  private static final String TASK_LOG_TABLE = "_tasklogs";
  private static final String TASK_LOCK_TABLE = "_tasklocks";

  @Test
  public void testFromBaseWithNull(){
    String defaultBase = "druid";
    DbTablesConfig defaultDbTablesConfigWithNull = DbTablesConfig.fromBase(null);
    Assert.assertEquals(defaultDbTablesConfigWithNull.getBase(), defaultBase);
    Assert.assertEquals(defaultDbTablesConfigWithNull.getSegmentsTable(), defaultBase + SEGMENTS_TABLES);
    Assert.assertEquals(defaultDbTablesConfigWithNull.getRulesTable(), defaultBase + RULES_TABLE);
    Assert.assertEquals(defaultDbTablesConfigWithNull.getConfigTable(), defaultBase + CONFIG_TABLE);
    Assert.assertEquals(defaultDbTablesConfigWithNull.getTasksTable(), defaultBase + TASKS_TABLE);
    Assert.assertEquals(defaultDbTablesConfigWithNull.getTaskLogTable(), defaultBase + TASK_LOG_TABLE);
    Assert.assertEquals(defaultDbTablesConfigWithNull.getTaskLockTable(),defaultBase + TASK_LOCK_TABLE);
  }

  @Test
  public void testFromBase()
  {
    String base = "base";
    DbTablesConfig defaultDbTablesConfig = DbTablesConfig.fromBase(base);
    Assert.assertEquals(defaultDbTablesConfig.getBase(), base);
    Assert.assertEquals(defaultDbTablesConfig.getSegmentsTable(), base + SEGMENTS_TABLES);
    Assert.assertEquals(defaultDbTablesConfig.getRulesTable(), base + RULES_TABLE);
    Assert.assertEquals(defaultDbTablesConfig.getConfigTable(), base + CONFIG_TABLE);
    Assert.assertEquals(defaultDbTablesConfig.getTasksTable(), base + TASKS_TABLE);
    Assert.assertEquals(defaultDbTablesConfig.getTaskLogTable(), base + TASK_LOG_TABLE);
    Assert.assertEquals(defaultDbTablesConfig.getTaskLockTable(),base + TASK_LOCK_TABLE);
  }

  @Test
  public void testDbTablesConfigWithNull()
  {
    String base = "BASE";
    String segmentsTables = base + "_SEGMENTS_TABLES";
    DbTablesConfig dbTablesConfig = new DbTablesConfig(
        base,
        segmentsTables,
        null,
        null,
        null,
        null,
        null
    );
    Assert.assertEquals(dbTablesConfig.getBase(), base);
    Assert.assertEquals(dbTablesConfig.getSegmentsTable(), segmentsTables);
    Assert.assertEquals(dbTablesConfig.getRulesTable(), base + RULES_TABLE);
    Assert.assertEquals(dbTablesConfig.getConfigTable(), base + CONFIG_TABLE);
    Assert.assertEquals(dbTablesConfig.getTasksTable(), base + TASKS_TABLE);
    Assert.assertEquals(dbTablesConfig.getTaskLogTable(), base + TASK_LOG_TABLE);
    Assert.assertEquals(dbTablesConfig.getTaskLockTable(),base + TASK_LOCK_TABLE);
  }
}