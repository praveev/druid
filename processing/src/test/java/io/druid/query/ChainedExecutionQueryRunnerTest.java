/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013, 2014  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.ListenableFuture;
import com.metamx.common.concurrent.ExecutorServiceConfig;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ChainedExecutionQueryRunnerTest
{

  CountDownLatch queriesStarted;
  CountDownLatch queriesInterrupted;
  CountDownLatch queryIsRegistered;
  DyingQueryRunner runner1;
  DyingQueryRunner runner2;
  DyingQueryRunner runner3;
  ExecutorService exec;
  ChainedExecutionQueryRunner chainedRunner;
  QueryWatcher watcher;
  Capture<ListenableFuture> capturedFuture;
  QueryInterruptedException cause;
  ListenableFuture future;
  static final int ONE_MINUTE = 60000;
  static final Logger log = new Logger(ChainedExecutionQueryRunnerTest.class);

  @Before
  public void Setup()
  {

    queriesStarted = new CountDownLatch(2);
    queriesInterrupted = new CountDownLatch(2);
    queryIsRegistered = new CountDownLatch(1);

    runner1 = new DyingQueryRunner(queriesStarted, queriesInterrupted, "runner_1");
    runner2 = new DyingQueryRunner(queriesStarted, queriesInterrupted, "runner_2");
    runner3 = new DyingQueryRunner(queriesStarted, queriesInterrupted, "runner_3");

    cause = null;
    exec = PrioritizedExecutorService.create(
        new Lifecycle(), new ExecutorServiceConfig()
        {
          @Override
          public String getFormatString()
          {
            return "test";
          }

          @Override
          public int getNumThreads()
          {
            return 2;
          }
        }
    );

    capturedFuture = new Capture<>();
    watcher = EasyMock.createStrictMock(QueryWatcher.class);
    watcher.registerQuery(EasyMock.<Query>anyObject(), EasyMock.and(EasyMock.<ListenableFuture>anyObject(), EasyMock.capture(capturedFuture)));
    EasyMock.expectLastCall()
        .andAnswer(
            new IAnswer<Void>()
            {
              @Override
              public Void answer() throws Throwable
              {
                queryIsRegistered.countDown();
                return null;
              }
            }
        )
        .once();

    EasyMock.replay(watcher);

    chainedRunner = new ChainedExecutionQueryRunner<>(
        exec,
        Ordering.<Integer>natural(),
        watcher,
        Lists.<QueryRunner<Integer>>newArrayList(
            runner1,
            runner2,
            runner3
        )
    );

  }

  @Test(timeout = ONE_MINUTE)
  public void testQueryCancellation() throws InterruptedException
  {

    final Sequence seq = chainedRunner.run(
        Druids.newTimeseriesQueryBuilder()
              .dataSource("test")
              .intervals("2014/2015")
              .aggregators(Lists.<AggregatorFactory>newArrayList(new CountAggregatorFactory("count")))
              .build()
    );
    Future resultFuture = Executors.newFixedThreadPool(1).submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            Sequences.toList(seq, Lists.newArrayList());
          }
        }
    );

    assertRegisterStartCancel();

    future = capturedFuture.getValue();
    future.cancel(true);
    try {
      resultFuture.get();
    } catch(ExecutionException e) {
      Assert.assertTrue("Exception should be instance of QueryInterruptedException",
          e.getCause() instanceof QueryInterruptedException);
      cause = (QueryInterruptedException)e.getCause();
    }

    assertTimeoutStartInterruptedCompleted();
  }

  @Test(timeout = ONE_MINUTE)
  public void testQueryTimeout() throws InterruptedException
  {

    final Sequence seq = chainedRunner.run(
        Druids.newTimeseriesQueryBuilder()
              .dataSource("test")
              .intervals("2014/2015")
              .aggregators(Lists.<AggregatorFactory>newArrayList(new CountAggregatorFactory("count")))
              .context(ImmutableMap.<String, Object>of("timeout", 100, "queryId", "test"))
              .build()
    );

    Future resultFuture = Executors.newFixedThreadPool(1).submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            Sequences.toList(seq, Lists.newArrayList());
          }
        }
    );

    assertRegisterStartCancel();

    future = capturedFuture.getValue();
    try {
      resultFuture.get();
    } catch(ExecutionException e) {
      Assert.assertTrue("Exception should be instance of QueryInterruptedException",
          e.getCause() instanceof QueryInterruptedException);
      Assert.assertEquals("Query timeout", e.getCause().getMessage());
      cause = (QueryInterruptedException)e.getCause();
    }

    assertTimeoutStartInterruptedCompleted();
  }

  private void assertRegisterStartCancel() throws InterruptedException
  {
    // wait for query to register and start
    log.info("Waiting for query to be registered");
    queryIsRegistered.await();
    log.info("Waiting for query to be started");
    queriesStarted.await();
    // cancel the query
    Assert.assertTrue("capturedFuture.hasCaptured() should return true",capturedFuture.hasCaptured());
  }

  private void assertTimeoutStartInterruptedCompleted() throws InterruptedException
  {
    log.info("Waiting for query to be interrupted");
    queriesInterrupted.await();
    Assert.assertNotNull("Cause must be not null", cause);
    Assert.assertTrue("future.isCancelled() must be true", future.isCancelled());
    Assert.assertTrue(runner1.toString() + " hasStarted should be true", runner1.hasStarted);
    Assert.assertTrue(runner2.toString() + " hasStarted should be true", runner2.hasStarted);
    Assert.assertTrue(runner1.toString() + " interrupted should be true", runner1.interrupted);
    Assert.assertTrue(runner2.toString() + " interrupted should be true", runner2.interrupted);
    Assert.assertTrue(runner3.toString() + " expected to not start and actual is " + runner3.hasStarted +
            " OR expected to be interrupted and actual is " + runner3.interrupted,
        !runner3.hasStarted || runner3.interrupted);
    Assert.assertFalse(runner1.toString() + " should not be completed", runner1.hasCompleted);
    Assert.assertFalse(runner2.toString() + " should not be completed", runner2.hasCompleted);
    Assert.assertFalse(runner3.toString() + " should not be completed", runner3.hasCompleted);
  }

  @After
  public void tearDown()
  {
    EasyMock.verify(watcher);
  }
  private static class DyingQueryRunner implements QueryRunner<Integer>
  {
    private final CountDownLatch start;
    private final CountDownLatch stop;
    private final String name;

    private volatile boolean hasStarted = false;
    private volatile boolean hasCompleted = false;
    private volatile boolean interrupted = false;

    public DyingQueryRunner(CountDownLatch start,CountDownLatch stop, String name)
    {
      this.start = start;
      this.stop = stop;
      this.name = name;
    }

    @Override
    public String toString()
    {
      return name;
    }

    @Override
    public Sequence<Integer> run(Query<Integer> query)
    {
      hasStarted = true;
      start.countDown();
      if (Thread.interrupted()) {
        interrupted = true;
        stop.countDown();
        throw new QueryInterruptedException("I got killed");
      }

      // do a lot of work
      try {
        Thread.sleep(500);
      }
      catch (InterruptedException e) {
        interrupted = true;
        stop.countDown();
        throw new QueryInterruptedException("I got killed");
      }

      hasCompleted = true;
      stop.countDown();
      return Sequences.simple(Lists.newArrayList(123));
    }
  }
}
