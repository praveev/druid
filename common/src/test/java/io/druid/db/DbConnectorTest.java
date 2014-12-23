package io.druid.db;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.exceptions.CallbackFailedException;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

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
      dbConnectorConfig = objectMapper.readValue(DB_CONNECTOR_CONFIG_STRING, DbConnectorConfig.class);
    } catch (Exception e)
    {
      Throwables.propagate(e);
    }
    String base = "base";
    dbTablesConfig = DbTablesConfig.fromBase(base);

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
    DbConnector dbConnector = new DbConnector(mockConfig, mockDbTables);
    Assert.assertNotNull(dbConnector.getDBI());
  }

  @Test
  public void testStaticIsPostgreSQL()
  {
    IDBI mockDbi = EasyMock.createMock(IDBI.class);
    EasyMock.expect(mockDbi.withHandle((HandleCallback<Object>) EasyMock.anyObject()))
        .andReturn(new Boolean(true)).anyTimes();
    EasyMock.replay(mockDbi);
    Assert.assertTrue(DbConnector.isPostgreSQL(mockDbi));
    EasyMock.verify(mockDbi);
  }

  @Test
  public void testCreateTables()
  {
    Map<String, String> mapOfTables = new HashMap<>();
    mapOfTables.put(
        mockDbTables.get().getRulesTable(),
        dbConnector.isPostgreSQL() ? RULE_TABLE_POSTGRE_QUERY : RULE_TABLE_SQL_QUERY
    );
    mapOfTables.put(
        mockDbTables.get().getSegmentsTable(),
        dbConnector.isPostgreSQL() ? SEGMENT_TABLE_POSTGRE_QUERY : SEGMENT_TABLE_SQL_QUERY
    );
    mapOfTables.put(
        mockDbTables.get().getConfigTable(),
        dbConnector.isPostgreSQL() ? CONFIG_TABLE_POSTGRE_QUERY : CONFIG_TABLE_SQL_QUERY
    );
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
      EasyMock.expect(mockHandel.select(String.format("SHOW tables LIKE '%s'", entry.getKey())))
          .andReturn(new ArrayList<Map<String,Object>>()).anyTimes();
      EasyMock.expect(
          mockHandel.createStatement(String.format(entry.getValue(), entry.getKey()))
      ).andReturn(null).anyTimes();
    }

    EasyMock.replay(mockHandel);
    dbConnector.createRulesTable();
    dbConnector.createSegmentTable();
    dbConnector.createConfigTable();
    dbConnector.createTaskTables();
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