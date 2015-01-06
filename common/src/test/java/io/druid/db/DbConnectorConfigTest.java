package io.druid.db;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DbConnectorConfigTest
{
  ObjectMapper objectMapper = new ObjectMapper();
  private DbConnectorConfig dbConnectorConfig;
  private final static Boolean CREATE_TABLES = true;
  private final static String CONNECT_URI = "URL";
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

  @Before
  public void setUp() throws Exception
  {
    dbConnectorConfig = objectMapper.readValue(
        objectMapper.writeValueAsBytes(objectMapper.readValue(
            DB_CONNECTOR_CONFIG_STRING,
            DbConnectorConfig.class)),
        DbConnectorConfig.class
    );
  }

  @Test
  public void testIsCreateTables()
  {
    Assert.assertEquals(CREATE_TABLES, dbConnectorConfig.isCreateTables());
  }

  @Test
  public void testGetConnectURI()
  {
    Assert.assertEquals(CONNECT_URI, dbConnectorConfig.getConnectURI());
  }

  @Test
  public void testGetUser()
  {
    Assert.assertEquals(USER, dbConnectorConfig.getUser());
  }

  @Test
  public void testGetPassword()
  {
    Assert.assertEquals(PASSWORD, dbConnectorConfig.getPassword());
  }

  @Test
  public void testIsUseValidationQuery()
  {
    Assert.assertEquals(USE_VALIDATION_QUERY, dbConnectorConfig.isUseValidationQuery());
  }

  @Test
  public void testGetValidationQuery()
  {
    Assert.assertEquals(VALIDATION_QUERY, dbConnectorConfig.getValidationQuery());
  }

  @Test
  public void testToString()
  {
    Assert.assertTrue(dbConnectorConfig.toString().contains("createTables=" + CREATE_TABLES.toString()));
    Assert.assertTrue(dbConnectorConfig.toString().contains("connectURI=\'" + CONNECT_URI + "\'"));
    Assert.assertTrue(dbConnectorConfig.toString().contains("user=\'" + USER + "\'"));
    Assert.assertTrue(dbConnectorConfig.toString().contains("validationQuery=\'" + VALIDATION_QUERY + "\'"));
    Assert.assertTrue(dbConnectorConfig.toString().contains("useValidationQuery=" + USE_VALIDATION_QUERY.toString() + ""));
  }
}