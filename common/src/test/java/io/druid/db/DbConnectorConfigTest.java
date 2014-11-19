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

  @Test
  public void testGetPassword_withPasswordProvidor()
  {
    String pwd = "nothing";
    DbConnectorConfig dbConfig = new DbConnectorConfig("pwd_key", "io.druid.db.DummyPasswordProvider");
    Assert.assertEquals(pwd, dbConfig.getPassword());

    //call again and ensure that provider initialization stores the password
    Assert.assertEquals(pwd, dbConfig.getPassword());
  }

  @Test
  public void testGetPassword_withPasswordProvidor_multipleThreads()
      throws ExecutionException, InterruptedException
  {
    String pwd = "nothing";
    final DbConnectorConfig dbConfig = new DbConnectorConfig("pwd_key", "io.druid.db.DummyPasswordProvider");

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
