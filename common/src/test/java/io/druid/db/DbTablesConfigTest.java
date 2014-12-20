package io.druid.db;

import org.hamcrest.Matchers;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

public class DbTablesConfigTest
{
  private static final String BASE = "druid";
  private static final String SEGMENTS_TABLES = BASE +"_segments";
  private static final String RULES_TABLE = BASE + "_rules";
  private static final String CONFIG_TABLE = BASE + "_config";
  private static final String TASKS_TABLE = BASE + "_tasks";
  private static final String TASK_LOG_TABLE = BASE + "_tasklogs";
  private static final String TASK_LOCK_TABLE = BASE + "_tasklocks";
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
    DbTablesConfig defaultDbTablesConfigWithNull = DbTablesConfig.fromBase(null);
    DbTablesConfig defaultDbTablesConfig = DbTablesConfig.fromBase(BASE);

    Assert.assertThat
        (
        dbTablesConfig.getBase(), CoreMatchers.allOf(
        Matchers.is(Matchers.equalTo(defaultDbTablesConfigWithNull.getBase())),
        Matchers.is(Matchers.equalTo(defaultDbTablesConfig.getBase())))
        );
    Assert.assertThat
        (
            dbTablesConfig.getSegmentsTable(), CoreMatchers.allOf(
                Matchers.is(Matchers.equalTo(defaultDbTablesConfigWithNull.getSegmentsTable())),
                Matchers.is(Matchers.equalTo(defaultDbTablesConfig.getSegmentsTable())))
        );
    Assert.assertThat
        (
            dbTablesConfig.getRulesTable(), CoreMatchers.allOf(
                Matchers.is(Matchers.equalTo(defaultDbTablesConfigWithNull.getRulesTable())),
                Matchers.is(Matchers.equalTo(defaultDbTablesConfig.getRulesTable())))
        );
    Assert.assertThat
        (
            dbTablesConfig.getConfigTable(), CoreMatchers.allOf(
                Matchers.is(Matchers.equalTo(defaultDbTablesConfigWithNull.getConfigTable())),
                Matchers.is(Matchers.equalTo(defaultDbTablesConfig.getConfigTable())))
        );
    Assert.assertThat
        (
            dbTablesConfig.getTasksTable(), CoreMatchers.allOf(
                Matchers.is(Matchers.equalTo(defaultDbTablesConfigWithNull.getTasksTable())),
                Matchers.is(Matchers.equalTo(defaultDbTablesConfig.getTasksTable())))
        );
    Assert.assertThat
        (
            dbTablesConfig.getTaskLogTable(), CoreMatchers.allOf(
                Matchers.is(Matchers.equalTo(defaultDbTablesConfigWithNull.getTaskLogTable())),
                Matchers.is(Matchers.equalTo(defaultDbTablesConfig.getTaskLogTable())))
        );
    Assert.assertThat
        (
            dbTablesConfig.getTaskLockTable(), CoreMatchers.allOf(
                Matchers.is(Matchers.equalTo(defaultDbTablesConfigWithNull.getTaskLockTable())),
                Matchers.is(Matchers.equalTo(defaultDbTablesConfig.getTaskLockTable())))
        );
    // Note that object are suppose to match because
    // values of BASE SEGMENTS_TABLES, RULE_TABLE,...,TASK_LOCK_TABLE reflect the default value in DbTablesConfig.fromBase() function
  }
}