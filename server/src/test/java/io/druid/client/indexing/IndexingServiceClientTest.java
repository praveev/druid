package io.druid.client.indexing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.metamx.common.IAE;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.RequestBuilder;
import com.metamx.http.client.response.HttpResponseHandler;
import io.druid.client.selector.Server;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.timeline.DataSegment;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class IndexingServiceClientTest
{
  private IndexingServiceClient indexingServiceClient;
  private HttpClient mockClient;
  private ServerDiscoverySelector mockServerDiscoverySelector;
  private Server serverInstance;
  private static final String DATA_SOURCE = "data_source";
  private static final Interval INTERVAL = new Interval(new DateTime(), new DateTime().plus(1));
  private static final DataSegment DATA_SEGMENT = new DataSegment(DATA_SOURCE, INTERVAL, new DateTime().toString(), null,
      null, null, null, 0, 0);
  private static final ObjectMapper objectMapper = new DefaultObjectMapper();
  private RequestBuilder mockRequestBuilder;

  @Before
  public void setUp() throws IOException
  {
    mockClient = EasyMock.createMock(HttpClient.class);
    mockRequestBuilder = EasyMock.createStrictMock(RequestBuilder.class);
    mockServerDiscoverySelector = EasyMock.createMock(ServerDiscoverySelector.class);
    serverInstance = EasyMock.createMock(Server.class);

    EasyMock.expect(serverInstance.getHost()).andReturn("localhost").anyTimes();
    EasyMock.replay(serverInstance);
    EasyMock.verify(serverInstance);

    EasyMock.expect(mockServerDiscoverySelector.pick()).andReturn(serverInstance).anyTimes();
    EasyMock.replay(mockServerDiscoverySelector);
    EasyMock.verify(mockServerDiscoverySelector);

    EasyMock.expect(mockClient.post(
        new URL("http://localhost/druid/indexer/v1/task"))).andReturn(mockRequestBuilder).anyTimes();
    EasyMock.replay(mockClient);
    EasyMock.verify(mockClient);

    indexingServiceClient = new IndexingServiceClient(mockClient, objectMapper, mockServerDiscoverySelector);
  }

  @Test(expected = IAE.class)
  public void testMergeSegmentsWithDifferentDataSource()
  {
    List<DataSegment> segments = ImmutableList
        .of(DataSegment.builder().dataSource("source_1").interval(new Interval("2012-01-01/P1D")).version("2").size(1)
                .build(),
            DataSegment.builder().dataSource("source_2").interval(new Interval("2012-01-02/P4D")).version("2").size(80)
                .build()
        );
    indexingServiceClient.mergeSegments(segments);
  }

  @Test
  public void testMergeSegments() throws IOException
  {
    List<DataSegment> segments = ImmutableList
        .of(DataSegment.builder().dataSource("source_1").interval(new Interval("2012-01-01/P1D")).version("2").size(1)
                .build(),
            DataSegment.builder().dataSource("source_1").interval(new Interval("2012-01-02/P4D")).version("2").size(1)
                .build()
        );

    EasyMock.expect(mockRequestBuilder.setContent(EasyMock.eq("application/json"), (byte[]) EasyMock.anyObject()))
        .andReturn(mockRequestBuilder).times(1);

    EasyMock.expect(mockRequestBuilder.go(EasyMock.isA(HttpResponseHandler.class))).andReturn(
        new ListenableFuture<InputStream>()
        {
          @Override public void addListener(Runnable listener, Executor executor)
          {

          }

          @Override public boolean cancel(boolean mayInterruptIfRunning)
          {
            return false;
          }

          @Override public boolean isCancelled()
          {
            return false;
          }

          @Override public boolean isDone()
          {
            return false;
          }

          @Override public InputStream get() throws InterruptedException, ExecutionException
          {
            return new InputStream()
            {
              @Override public int read() throws IOException
              {
                return 0;
              }
            };
          }

          @Override public InputStream get(long timeout, TimeUnit unit)
              throws InterruptedException, ExecutionException, TimeoutException
          {
            return null;
          }
        }).times(1);
    EasyMock.replay(mockRequestBuilder);

    indexingServiceClient.mergeSegments(segments);
    EasyMock.verify(mockRequestBuilder);
  }

  @Test
  public void testKillSegments() throws IOException
  {
    EasyMock.expect(mockRequestBuilder.setContent(EasyMock.eq("application/json"),
        EasyMock.aryEq(objectMapper.writeValueAsBytes(new ClientKillQuery(DATA_SOURCE, INTERVAL)))))
        .andReturn(mockRequestBuilder).times(1);

    EasyMock.expect(mockRequestBuilder.go(EasyMock.isA(HttpResponseHandler.class))).andReturn(
        new ListenableFuture<InputStream>()
        {
          @Override public void addListener(Runnable listener, Executor executor)
          {

          }

          @Override public boolean cancel(boolean mayInterruptIfRunning)
          {
            return false;
          }

          @Override public boolean isCancelled()
          {
            return false;
          }

          @Override public boolean isDone()
          {
            return false;
          }

          @Override public InputStream get() throws InterruptedException, ExecutionException
          {
            return new InputStream()
            {
              @Override public int read() throws IOException
              {
                return 0;
              }
            };
          }

          @Override public InputStream get(long timeout, TimeUnit unit)
              throws InterruptedException, ExecutionException, TimeoutException
          {
            return null;
          }
        }).times(1);
    EasyMock.replay(mockRequestBuilder);

    indexingServiceClient.killSegments(DATA_SOURCE,INTERVAL);
    EasyMock.verify(mockRequestBuilder);
  }

  @Test
  public void testUpgradeSegment() throws IOException
  {
    EasyMock.expect(mockRequestBuilder.setContent(EasyMock.eq("application/json"),
        EasyMock.aryEq(objectMapper.writeValueAsBytes(new ClientConversionQuery(DATA_SEGMENT)))))
        .andReturn(mockRequestBuilder).times(1);

    EasyMock.expect(mockRequestBuilder.go(EasyMock.isA(HttpResponseHandler.class))).andReturn(
        new ListenableFuture<InputStream>()
        {
          @Override public void addListener(Runnable listener, Executor executor)
          {

          }

          @Override public boolean cancel(boolean mayInterruptIfRunning)
          {
            return false;
          }

          @Override public boolean isCancelled()
          {
            return false;
          }

          @Override public boolean isDone()
          {
            return false;
          }

          @Override public InputStream get() throws InterruptedException, ExecutionException
          {
            return new InputStream()
            {
              @Override public int read() throws IOException
              {
                return 0;
              }
            };
          }

          @Override public InputStream get(long timeout, TimeUnit unit)
              throws InterruptedException, ExecutionException, TimeoutException
          {
            return null;
          }
        }).times(1);
    EasyMock.replay(mockRequestBuilder);
    indexingServiceClient.upgradeSegment(DATA_SEGMENT);
    EasyMock.verify(mockRequestBuilder);
  }

  @Test
  public void testUpgradeSegments() throws IOException
  {
    EasyMock.expect(mockRequestBuilder.setContent(EasyMock.eq("application/json"),
        EasyMock.aryEq(objectMapper.writeValueAsBytes(new ClientConversionQuery(DATA_SOURCE,INTERVAL)))))
        .andReturn(mockRequestBuilder).times(1);

    EasyMock.expect(mockRequestBuilder.go(EasyMock.isA(HttpResponseHandler.class))).andReturn(
        new ListenableFuture<InputStream>()
        {
          @Override public void addListener(Runnable listener, Executor executor)
          {

          }

          @Override public boolean cancel(boolean mayInterruptIfRunning)
          {
            return false;
          }

          @Override public boolean isCancelled()
          {
            return false;
          }

          @Override public boolean isDone()
          {
            return false;
          }

          @Override public InputStream get() throws InterruptedException, ExecutionException
          {
            return new InputStream()
            {
              @Override public int read() throws IOException
              {
                return 0;
              }
            };
          }

          @Override public InputStream get(long timeout, TimeUnit unit)
              throws InterruptedException, ExecutionException, TimeoutException
          {
            return null;
          }
        }).times(1);
    EasyMock.replay(mockRequestBuilder);
    indexingServiceClient.upgradeSegments(DATA_SOURCE, INTERVAL);
    EasyMock.verify(mockRequestBuilder);
  }
}