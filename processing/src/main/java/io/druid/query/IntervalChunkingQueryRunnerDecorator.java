package io.druid.query;

import io.druid.guice.annotations.Processing;

import java.util.concurrent.ExecutorService;

import com.google.inject.Inject;
import com.metamx.emitter.service.ServiceEmitter;

public class IntervalChunkingQueryRunnerDecorator implements QueryRunnerDecorator
{
  private final ExecutorService executor;
  private final QueryWatcher queryWatcher;
  private final ServiceEmitter emitter;

  @Inject
  public IntervalChunkingQueryRunnerDecorator(@Processing ExecutorService executor, QueryWatcher queryWatcher,
      ServiceEmitter emitter)
  {
    this.executor = executor;
    this.queryWatcher = queryWatcher;
    this.emitter = emitter;
  }

  @Override
  public <T> QueryRunner<T> decorate(QueryRunner<T> delegate,
      QueryToolChest<T, ? extends Query<T>> toolChest) {
    return new IntervalChunkingQueryRunner<T>(delegate, (QueryToolChest<T, Query<T>>)toolChest,
        executor, queryWatcher, emitter);
  }
}
