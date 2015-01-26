package io.druid.query;

public interface QueryRunnerDecorator
{
  public <T> QueryRunner<T> decorate(QueryRunner<T> delegate,
      QueryToolChest<T, ? extends Query<T>> toolChest);
}
