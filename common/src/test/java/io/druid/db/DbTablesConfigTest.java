package io.druid.db;

import org.junit.Assert;
import org.junit.Test;

public class DbTablesConfigTest
{
  private static final String BASE = "druid";
  private static final String SEGMENTS_TABLES = BASE+"_segments";
  private static final String RULES_TABLE = BASE+"_rules";
  private static final String CONFIG_TABLE = BASE+"_config";
  private static final String TASKS_TABLE = BASE+"_tasks";
  private static final String TASK_LOG_TABLE = BASE+"_tasklogs";
  private static final String TASK_LOCK_TABLE = BASE+"_tasklocks";
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
    DbTablesConfig defaultDbTablesConfig = DbTablesConfig.fromBase(null);
    // Note that object are suppose to match because
    // values of BASE SEGMENTS_TABLES, RULE_TABLE,...,TASK_LOCK_TABLE reflect the default value in DbTablesConfig.fromBase() function
    Assert.assertEquals(dbTablesConfig.getBase(), defaultDbTablesConfig.getBase());
    Assert.assertEquals(dbTablesConfig.getSegmentsTable(), defaultDbTablesConfig.getSegmentsTable());
    Assert.assertEquals(dbTablesConfig.getRulesTable(), defaultDbTablesConfig.getRulesTable());
    Assert.assertEquals(dbTablesConfig.getConfigTable(), defaultDbTablesConfig.getConfigTable());
    Assert.assertEquals(dbTablesConfig.getTasksTable(), defaultDbTablesConfig.getTasksTable());
    Assert.assertEquals(dbTablesConfig.getTaskLogTable(), defaultDbTablesConfig.getTaskLogTable());
    Assert.assertEquals(dbTablesConfig.getTaskLockTable(), defaultDbTablesConfig.getTaskLockTable());
  }
}