package io.druid.db;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.exceptions.CallbackFailedException;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(JUnitParamsRunner.class)
public class DbConnectorTest
{
  ObjectMapper objectMapper = new ObjectMapper();
  private DbConnectorConfig dbConnectorConfig;
  private IDBI mockIDBI;
  private DbTablesConfig dbTablesConfig;
  private DbConnector dbConnector;

  private final static Boolean CREATE_TABLES = true;
  private final static String CONNECT_URI = "jdbc:mysql://localhost/druid";
  private final static String USER = "user";
  private final static String VALIDATION_QUERY = "SELECT 1";
  private final static Boolean USE_VALIDATION_QUERY = true;
  private final static String PASSWORD = "trulydummy";
  private final static String DB_CONNECTOR_CONFIG_STRING ="{"
      + "\"createTables\":\"" + CREATE_TABLES.toString() +"\","
      + "\"connectURI\": \"" + CONNECT_URI + "\","
      + "\"user\": \"" + USER + "\","
      + "\"validationQuery\": \"" + VALIDATION_QUERY + "\","
      + "\"useValidationQuery\": \"" + USE_VALIDATION_QUERY + "\","
      + "\"password\" : {\"type\":\"default\",\"password\":\"" + PASSWORD + "\"}"
      + "}" ;

  protected static final String SEGMENT_TABLE_SQL_QUERY = "CREATE table %s (id VARCHAR(255) NOT NULL, dataSource VARCHAR(255) NOT NULL, created_date TINYTEXT NOT NULL, start TINYTEXT NOT NULL, end TINYTEXT NOT NULL, partitioned BOOLEAN NOT NULL, version TINYTEXT NOT NULL, used BOOLEAN NOT NULL, payload LONGTEXT NOT NULL, INDEX(dataSource), INDEX(used), PRIMARY KEY (id))";
  protected static final String SEGMENT_TABLE_POSTGRE_QUERY = "CREATE TABLE %1$s (id VARCHAR(255) NOT NULL, dataSource VARCHAR(255) NOT NULL, created_date TEXT NOT NULL, start TEXT NOT NULL, \"end\" TEXT NOT NULL, partitioned SMALLINT NOT NULL, version TEXT NOT NULL, used BOOLEAN NOT NULL, payload bytea NOT NULL, PRIMARY KEY (id));" +
      "CREATE INDEX ON %1$s(dataSource);"+
      "CREATE INDEX ON %1$s(used);";

  protected static final String RULE_TABLE_SQL_QUERY = "CREATE table %s (id VARCHAR(255) NOT NULL, dataSource VARCHAR(255) NOT NULL, version TINYTEXT NOT NULL, payload LONGTEXT NOT NULL, INDEX(dataSource), PRIMARY KEY (id))";
  protected static final String RULE_TABLE_POSTGRE_QUERY = "CREATE TABLE %1$s (id VARCHAR(255) NOT NULL, dataSource VARCHAR(255) NOT NULL, version TEXT NOT NULL, payload bytea NOT NULL, PRIMARY KEY (id));"+
      "CREATE INDEX ON %1$s(dataSource);";

  protected static final String CONFIG_TABLE_SQL_QUERY = "CREATE table %s (name VARCHAR(255) NOT NULL, payload BLOB NOT NULL, PRIMARY KEY(name))";
  protected static final String CONFIG_TABLE_POSTGRE_QUERY = "CREATE TABLE %s (name VARCHAR(255) NOT NULL, payload bytea NOT NULL, PRIMARY KEY(name))";

  protected static final String TASK_TABLE_SQL_QUERY =
      "CREATE TABLE `%s` (\n"
          + "  `id` varchar(255) NOT NULL,\n"
          + "  `created_date` tinytext NOT NULL,\n"
          + "  `datasource` varchar(255) NOT NULL,\n"
          + "  `payload` longblob NOT NULL,\n"
          + "  `status_payload` longblob NOT NULL,\n"
          + "  `active` tinyint(1) NOT NULL DEFAULT '0',\n"
          + "  PRIMARY KEY (`id`),\n"
          + "  KEY (active, created_date(100))\n"
          + ")";
  protected static final String TASK_TABLE_POSTGRE_QUERY =
      "CREATE TABLE %1$s (\n"
          + "  id varchar(255) NOT NULL,\n"
          + "  created_date TEXT NOT NULL,\n"
          + "  datasource varchar(255) NOT NULL,\n"
          + "  payload bytea NOT NULL,\n"
          + "  status_payload bytea NOT NULL,\n"
          + "  active SMALLINT NOT NULL DEFAULT '0',\n"
          + "  PRIMARY KEY (id)\n"
          + ");\n" +
          "CREATE INDEX ON %1$s(active, created_date);";

  protected static final String TASK_LOG_TABLE_SQL_QUERY =
      "CREATE TABLE `%s` (\n"
          + "  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
          + "  `task_id` varchar(255) DEFAULT NULL,\n"
          + "  `log_payload` longblob,\n"
          + "  PRIMARY KEY (`id`),\n"
          + "  KEY `task_id` (`task_id`)\n"
          + ")";
  protected static final String TASK_LOG_TABLE_POSTGRE_QUERY =
      "CREATE TABLE %1$s (\n"
          + "  id bigserial NOT NULL,\n"
          + "  task_id varchar(255) DEFAULT NULL,\n"
          + "  log_payload bytea,\n"
          + "  PRIMARY KEY (id)\n"
          + ");\n"+
          "CREATE INDEX ON %1$s(task_id);";

  protected static final String TASK_LOCK_TABLE_SQL_QUERY =
      "CREATE TABLE `%s` (\n"
          + "  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
          + "  `task_id` varchar(255) DEFAULT NULL,\n"
          + "  `lock_payload` longblob,\n"
          + "  PRIMARY KEY (`id`),\n"
          + "  KEY `task_id` (`task_id`)\n"
          + ")";
  protected static final String TASK_LOCK_TABLE_POSTGRE_QUERY =
      "CREATE TABLE %1$s (\n"
          + "  id bigserial NOT NULL,\n"
          + "  task_id varchar(255) DEFAULT NULL,\n"
          + "  lock_payload bytea,\n"
          + "  PRIMARY KEY (id)\n"
          + ");\n"+
          "CREATE INDEX ON %1$s(task_id);";

  private final Handle mockHandel = EasyMock.createMock(Handle.class);
  private Supplier<DbConnectorConfig> mockConfig = EasyMock.createMock(Supplier.class);
  private Supplier<DbTablesConfig> mockDbTables = EasyMock.createMock(Supplier.class);

  @Before
  public void setUp()
  {
    try
    {
      dbConnectorConfig = objectMapper.readValue(
          objectMapper.writeValueAsBytes(objectMapper.readValue(DB_CONNECTOR_CONFIG_STRING, DbConnectorConfig.class)),
          DbConnectorConfig.class);
    } catch (Exception e)
    {
      Throwables.propagate(e);
    }
    dbTablesConfig = DbTablesConfig.fromBase("base");

    EasyMock.expect(mockConfig.get()).andReturn(dbConnectorConfig).anyTimes();
    EasyMock.replay(mockConfig);
    EasyMock.expect(mockDbTables.get()).andReturn(dbTablesConfig).anyTimes();
    EasyMock.replay(mockDbTables);

    mockIDBI = new MockDbi(mockHandel);
    dbConnector = new DbConnector(mockConfig, mockDbTables, mockIDBI);
  }

  @Test
  public void testGetDBI()
  {
    Assert.assertEquals(mockIDBI, dbConnector.getDBI());
  }

  @Test
  @Parameters({"PostgreSQL, true","doesn't contain <Postgre SQL> as one word, false"})
  public void testStaticIsPostgreSQL(String productName, boolean expected) throws SQLException
  {
    Connection mockConnection = EasyMock.createMock(Connection.class);
    DatabaseMetaData mockDatabaseMetaData = EasyMock.createMock(DatabaseMetaData.class);

    EasyMock.expect(mockHandel.getConnection()).andReturn(mockConnection).anyTimes();
    EasyMock.expect(mockConnection.getMetaData()).andReturn(mockDatabaseMetaData).anyTimes();
    EasyMock.expect(mockDatabaseMetaData.getDatabaseProductName()).andReturn(productName).anyTimes();

    EasyMock.replay(mockHandel, mockConnection, mockDatabaseMetaData);
    Assert.assertEquals(expected, DbConnector.isPostgreSQL(mockIDBI));
    EasyMock.verify(mockHandel, mockConnection, mockDatabaseMetaData);
  }

  private void loadMockHandelExpectations(String tableName, String sql){
    EasyMock.expect(mockHandel.select(String.format("SHOW tables LIKE '%s'", tableName)))
        .andReturn(new ArrayList<Map<String,Object>>()).anyTimes();
    EasyMock.expect(
        mockHandel.createStatement(String.format(sql, tableName))
    ).andReturn(null).anyTimes();
  }

  @Test
  public void testCreateRulesTable()
  {
    String tableName = mockDbTables.get().getRulesTable();
    String sqlStatement = dbConnector.isPostgreSQL() ? RULE_TABLE_POSTGRE_QUERY : RULE_TABLE_SQL_QUERY;
    loadMockHandelExpectations(tableName, sqlStatement);
    EasyMock.replay(mockHandel);
    dbConnector.createRulesTable();
    EasyMock.verify(mockHandel);
  }

  @Test
  public void testCreateSegmentTable()
  {
    String tableName = mockDbTables.get().getSegmentsTable();
    String sqlStatement = dbConnector.isPostgreSQL() ? SEGMENT_TABLE_POSTGRE_QUERY : SEGMENT_TABLE_SQL_QUERY;
    loadMockHandelExpectations(tableName, sqlStatement);
    EasyMock.replay(mockHandel);
    dbConnector.createSegmentTable();
    EasyMock.verify(mockHandel);
  }

  @Test
  public void testCreateConfigTable()
  {
    String tableName = mockDbTables.get().getConfigTable();
    String sqlStatement = dbConnector.isPostgreSQL() ? CONFIG_TABLE_POSTGRE_QUERY : CONFIG_TABLE_SQL_QUERY;
    loadMockHandelExpectations(tableName, sqlStatement);
    EasyMock.replay(mockHandel);
    dbConnector.createConfigTable();
    EasyMock.verify(mockHandel);
  }

  @Test
  public void testCreateTaskTables()
  {
    Map<String, String> mapOfTables = new HashMap<>();
    mapOfTables.put(
        mockDbTables.get().getTasksTable(),
        dbConnector.isPostgreSQL() ? TASK_TABLE_POSTGRE_QUERY : TASK_TABLE_SQL_QUERY
    );
    mapOfTables.put(
        mockDbTables.get().getTaskLogTable(),
        dbConnector.isPostgreSQL() ? TASK_LOG_TABLE_POSTGRE_QUERY : TASK_LOG_TABLE_SQL_QUERY
    );
    mapOfTables.put(
        mockDbTables.get().getTaskLockTable(),
        dbConnector.isPostgreSQL() ? TASK_LOCK_TABLE_POSTGRE_QUERY : TASK_LOCK_TABLE_SQL_QUERY
    );

    for( Map.Entry<String,String> entry : mapOfTables.entrySet() ) {
      loadMockHandelExpectations(entry.getKey(),entry.getValue());
    }

    EasyMock.replay(mockHandel);
    dbConnector.createTaskTables();
    EasyMock.verify(mockHandel);
  }

  @Test
  public void testCreateTableCaseTableExist()
  {
    String tableName = "tableName";
    List result = new ArrayList();
    result.add(1);
    EasyMock.expect(mockHandel.select(String.format("SHOW tables LIKE '%s'", tableName))).andReturn(result).anyTimes();
    EasyMock.replay(mockHandel);
    DbConnector.createTable(mockIDBI, tableName, null, false);
    EasyMock.verify(mockHandel);
  }

  private class MockDbi implements IDBI
  {
    Handle handle;
    public MockDbi(Handle handle)
    {
      this.handle = handle;
    }
    @Override public Handle open()
    {
      return null;
    }

    @Override public void define(String key, Object value)
    {

    }

    @Override public <ReturnType> ReturnType withHandle(HandleCallback<ReturnType> callback)
        throws CallbackFailedException
    {
      try
      {
        return callback.withHandle(handle);
      } catch (Exception e)
      {
        Throwables.propagate(e);
        return  null;
      }
    }

    @Override public <ReturnType> ReturnType inTransaction(TransactionCallback<ReturnType> callback)
        throws CallbackFailedException
    {
      return null;
    }

    @Override public <SqlObjectType> SqlObjectType open(Class<SqlObjectType> sqlObjectType)
    {
      return null;
    }

    @Override public <SqlObjectType> SqlObjectType onDemand(Class<SqlObjectType> sqlObjectType)
    {
      return null;
    }

    @Override public void close(Object sqlObject)
    {

    }
  }
}