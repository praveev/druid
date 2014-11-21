package io.druid.db;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Assert;
import org.junit.Test;

import com.metamx.common.IAE;

public class DbConnectorConfigTest
{

  @Test(expected = IAE.class)
  public void testGetPassword_nullPasswordAndProvider()
  {
    DbConnectorConfig dbConfig = new DbConnectorConfig(null,null,null);
    dbConfig.getPassword();
  }

  @Test
  public void testGetPassword_withSimplePassword()
  {
    String pwd = "nothing";
    DbConnectorConfig dbConfig = new DbConnectorConfig(null,null,pwd);
    Assert.assertEquals(pwd, dbConfig.getPassword());
  }

  @Test
  public void testGetPassword_withPasswordProvidor()
  {
    String pwd = "nothing";
    DbConnectorConfig dbConfig = new DbConnectorConfig("io.druid.db.DummyPasswordProvider",
        "pwd:" + pwd, null);

    Assert.assertEquals(pwd, dbConfig.getPassword());
    Assert.assertEquals(pwd, dbConfig.getPassword()); //ensures that provider initialization stores the password
  }

  @Test
  public void testGetPassword_withPasswordProvidor_multipleThreads()
      throws ExecutionException, InterruptedException
  {
    String pwd = "nothing";
    final DbConnectorConfig dbConfig = new DbConnectorConfig("io.druid.db.DummyPasswordProvider"
        ,"pwd:"+pwd,null);

    int nThreads = 5;
    ExecutorService es = Executors.newFixedThreadPool(nThreads);
    List<Future<String>> futures = new ArrayList<>();
    for (int i = 0; i < nThreads; i++) {
      futures.add(es.submit(new Callable<String>()
      {
        public String call() throws Exception
        {
          return dbConfig.getPassword();
        }
      }));
    }
    for(Future<String> f : futures) {
      Assert.assertEquals(pwd, f.get());
    }
  }
}
