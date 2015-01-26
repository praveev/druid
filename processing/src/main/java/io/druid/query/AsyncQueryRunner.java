package io.druid.query;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.guava.LazySequence;
import com.metamx.common.guava.Sequence;

public class AsyncQueryRunner<T> implements QueryRunner<T>
{

  private final QueryRunner<T> baseRunner;
  private final ListeningExecutorService executor;
  private final QueryWatcher queryWatcher;

  public AsyncQueryRunner(QueryRunner<T> baseRunner, ExecutorService executor, QueryWatcher queryWatcher) {
    this.baseRunner = baseRunner;
    this.executor = MoreExecutors.listeningDecorator(executor);
    this.queryWatcher = queryWatcher;
  }

  @Override
  public Sequence<T> run(final Query<T> query)
  {
    final int priority = query.getContextPriority(0);
    final ListenableFuture<Sequence<T>> future = executor.submit(new AbstractPrioritizedCallable<Sequence<T>>(priority)
        {
          @Override
          public Sequence<T> call() throws Exception
          {
            //Note: this is assumed that baseRunner does most of the work eagerly on call to the
            //run() method and is not lazy i.e. does not do any work on call to run() but actually
            //when sequence is accumulated/yielded
            return baseRunner.run(query);
          }
        });
    queryWatcher.registerQuery(query, future);
    
    return new LazySequence<>(new Supplier<Sequence<T>>()
    {
      @Override
      public Sequence<T> get()
      {
        try {
          Number timeout = query.getContextValue(QueryContextKeys.TIMEOUT);
          if (timeout == null) {
            return future.get();
          } else {
            return future.get(timeout.longValue(), TimeUnit.MILLISECONDS);
          }
        } catch (ExecutionException | InterruptedException | TimeoutException ex) {
          throw Throwables.propagate(ex);
        }
      }
    });
  }
}
