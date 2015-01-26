package io.druid.query;

import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;

public class AsyncQueryRunnerTest
{

  private final static long TEST_TIMEOUT = 60000;
  
  private final ExecutorService executor;
  private final Query query;

  public AsyncQueryRunnerTest() {
    this.executor = Executors.newSingleThreadExecutor();
    query = Druids.newTimeseriesQueryBuilder()
              .dataSource("test")
              .intervals("2014/2015")
              .aggregators(Lists.<AggregatorFactory>newArrayList(new CountAggregatorFactory("count")))
              .build();
  }
  
  @Test(timeout = TEST_TIMEOUT)
  public void testAsyncNature() throws Exception {
    final CountDownLatch latch = new CountDownLatch(1);
    QueryRunner baseRunner = new QueryRunner()
    {
      @Override
      public Sequence run(Query query)
      {
        try {
          latch.await();
          return Sequences.simple(Lists.newArrayList(1));
        } catch(InterruptedException ex) {
          throw Throwables.propagate(ex);
        }
      }
    };
    
    AsyncQueryRunner asyncRunner = new AsyncQueryRunner<>(baseRunner, executor,
        QueryRunnerTestHelper.NOOP_QUERYWATCHER);
    
    Sequence lazy = asyncRunner.run(query);
    latch.countDown();
    Assert.assertEquals(Lists.newArrayList(1), Sequences.toList(lazy, Lists.newArrayList()));
  }
  
  @Test(timeout = TEST_TIMEOUT)
  public void testQueryTimeoutHonored() {
    QueryRunner baseRunner = new QueryRunner()
    {
      @Override
      public Sequence run(Query query)
      {
        try {
          Thread.sleep(Long.MAX_VALUE);
          throw new RuntimeException("query should not have completed");
        } catch(InterruptedException ex) {
          throw Throwables.propagate(ex);
        }
      }
    };
    
    AsyncQueryRunner asyncRunner = new AsyncQueryRunner<>(baseRunner, executor,
        QueryRunnerTestHelper.NOOP_QUERYWATCHER);

    Sequence lazy = asyncRunner.run(
        query.withOverriddenContext(ImmutableMap.<String,Object>of("timeout", 1)));

    try {
      Sequences.toList(lazy, Lists.newArrayList());
    } catch(RuntimeException ex) {
      Assert.assertTrue(ex.getCause() instanceof TimeoutException);
      return;
    }
    Assert.fail();
  }

  @Test
  public void testQueryRegistration() {
    QueryRunner baseRunner = new QueryRunner()
    {
      @Override
      public Sequence run(Query query) { return null; }
    };

    QueryWatcher mock = EasyMock.createMock(QueryWatcher.class);
    mock.registerQuery(EasyMock.eq(query), EasyMock.anyObject(ListenableFuture.class));
    EasyMock.replay(mock);

    AsyncQueryRunner asyncRunner = new AsyncQueryRunner<>(baseRunner, executor,
        mock);
    
    asyncRunner.run(query);
    EasyMock.verify(mock);
  }
}