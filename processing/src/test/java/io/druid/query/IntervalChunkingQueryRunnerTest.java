package io.druid.query;

import io.druid.query.Druids.TimeseriesQueryBuilder;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;

import java.util.concurrent.ExecutorService;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.common.guava.Sequences;
import com.metamx.emitter.service.ServiceEmitter;

public class IntervalChunkingQueryRunnerTest
{
  private IntervalChunkingQueryRunnerDecorator decorator;
  private ExecutorService executors;
  private QueryRunner baseRunner;
  private QueryToolChest toolChest;

  private final TimeseriesQueryBuilder queryBuilder;

  public IntervalChunkingQueryRunnerTest() {
    queryBuilder = Druids.newTimeseriesQueryBuilder()
              .dataSource("test")
              .aggregators(Lists.<AggregatorFactory>newArrayList(new CountAggregatorFactory("count")));
  }

  @Before
  public void setup() {
    executors = EasyMock.createMock(ExecutorService.class);
    ServiceEmitter emitter = EasyMock.createNiceMock(ServiceEmitter.class);
    decorator = new IntervalChunkingQueryRunnerDecorator(executors,
        QueryRunnerTestHelper.NOOP_QUERYWATCHER, emitter);
    baseRunner = EasyMock.createMock(QueryRunner.class);
    toolChest = EasyMock.createNiceMock(QueryToolChest.class);
  }

  @Test
  public void testDefaultNoChunking() {
    Query query = queryBuilder.intervals("2014/2016").build();

    EasyMock.expect(baseRunner.run(query)).andReturn(Sequences.empty());
    EasyMock.replay(baseRunner);

    QueryRunner runner = decorator.decorate(baseRunner, toolChest);
    runner.run(query);

    EasyMock.verify(baseRunner);
  }

  @Test
  public void testChunking() {
    Query query = queryBuilder.intervals("2015-01-01T00:00:00.000/2015-01-11T00:00:00.000").context(ImmutableMap.<String, Object>of("chunkPeriod", "P1D")).build();

    executors.execute(EasyMock.anyObject(Runnable.class));
    EasyMock.expectLastCall().times(10);

    EasyMock.replay(executors);
    EasyMock.replay(toolChest);
    
    QueryRunner runner = decorator.decorate(baseRunner, toolChest);
    runner.run(query);

    EasyMock.verify(executors);
  }
}
