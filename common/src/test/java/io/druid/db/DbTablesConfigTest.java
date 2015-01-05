package io.druid.db;

import org.hamcrest.Matchers;
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
    DbTablesConfig dbTablesConfig = new DbTablesConfig(
        defaultBase,
        defaultBase + SEGMENTS_TABLES,
        defaultBase + RULES_TABLE,
        defaultBase + CONFIG_TABLE,
        defaultBase + TASKS_TABLE,
        defaultBase + TASK_LOG_TABLE,
        defaultBase + TASK_LOCK_TABLE
    );
    DbTablesConfig defaultDbTablesConfigWithNull = DbTablesConfig.fromBase(null);
    Assert.assertThat
        (dbTablesConfig.getBase(), Matchers.is(Matchers.equalTo(defaultDbTablesConfigWithNull.getBase())));
    Assert.assertThat
        (dbTablesConfig.getSegmentsTable(), Matchers.is(Matchers.equalTo(defaultDbTablesConfigWithNull.getSegmentsTable())));
    Assert.assertThat
        (dbTablesConfig.getRulesTable(), Matchers.is(Matchers.equalTo(defaultDbTablesConfigWithNull.getRulesTable())));
    Assert.assertThat
        (dbTablesConfig.getConfigTable(), Matchers.is(Matchers.equalTo(defaultDbTablesConfigWithNull.getConfigTable())));
    Assert.assertThat
        (dbTablesConfig.getTasksTable(), Matchers.is(Matchers.equalTo(defaultDbTablesConfigWithNull.getTasksTable())));
    Assert.assertThat
        (dbTablesConfig.getTaskLogTable(), Matchers.is(Matchers.equalTo(defaultDbTablesConfigWithNull.getTaskLogTable())));
    Assert.assertThat
        (dbTablesConfig.getTaskLockTable(), Matchers.is(Matchers.equalTo(defaultDbTablesConfigWithNull.getTaskLockTable())));


  }

  @Test
  public void testFromBase()
  {
    String base = "base";
    DbTablesConfig dbTablesConfig = new DbTablesConfig(
        base,
        base + SEGMENTS_TABLES,
        base + RULES_TABLE,
        base + CONFIG_TABLE,
        base + TASKS_TABLE,
        base + TASK_LOG_TABLE,
        base + TASK_LOCK_TABLE
    );
    DbTablesConfig defaultDbTablesConfig = DbTablesConfig.fromBase(base);
    Assert.assertThat
        (dbTablesConfig.getBase(), Matchers.is(Matchers.equalTo(defaultDbTablesConfig.getBase())));
    Assert.assertThat
        (dbTablesConfig.getSegmentsTable(), Matchers.is(Matchers.equalTo(defaultDbTablesConfig.getSegmentsTable())));
    Assert.assertThat
        (dbTablesConfig.getRulesTable(), Matchers.is(Matchers.equalTo(defaultDbTablesConfig.getRulesTable())));
    Assert.assertThat
        (dbTablesConfig.getConfigTable(), Matchers.is(Matchers.equalTo(defaultDbTablesConfig.getConfigTable())));
    Assert.assertThat
        (dbTablesConfig.getTasksTable(), Matchers.is(Matchers.equalTo(defaultDbTablesConfig.getTasksTable())));
    Assert.assertThat
        (dbTablesConfig.getTaskLogTable(), Matchers.is(Matchers.equalTo(defaultDbTablesConfig.getTaskLogTable())));
    Assert.assertThat
        (dbTablesConfig.getTaskLockTable(), Matchers.is(Matchers.equalTo(defaultDbTablesConfig.getTaskLockTable())));
  }
}