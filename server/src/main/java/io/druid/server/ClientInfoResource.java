/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.server;

import io.druid.client.DruidDataSource;
import io.druid.client.DruidServer;
import io.druid.client.InventoryView;
import io.druid.client.TimelineServerView;
import io.druid.client.selector.ServerSelector;
import io.druid.query.TableDataSource;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import org.joda.time.DateTime;
import org.joda.time.Interval;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;

/**
 */
@Path("/druid/v2/datasources")
public class ClientInfoResource
{
  private static Logger log = new Logger(ClientInfoResource.class);

  private static final int SEGMENT_HISTORY_MILLIS = 7 * 24 * 60 * 60 * 1000; // ONE WEEK

  private static final String KEY_DIMENSIONS = "dimensions";
  private static final String KEY_METRICS = "metrics";
  
  private InventoryView serverInventoryView;
  private TimelineServerView timelineServerView;

  @Inject
  public ClientInfoResource(
      InventoryView serverInventoryView,
      TimelineServerView timelineServerView
  )
  {
    this.serverInventoryView = serverInventoryView;
    this.timelineServerView = timelineServerView;
  }

  private Map<String, List<DataSegment>> getSegmentsForDatasources()
  {
    final Map<String, List<DataSegment>> dataSourceMap = Maps.newHashMap();
    for (DruidServer server : serverInventoryView.getInventory()) {
      for (DruidDataSource dataSource : server.getDataSources()) {
        if (!dataSourceMap.containsKey(dataSource.getName())) {
          dataSourceMap.put(dataSource.getName(), Lists.<DataSegment>newArrayList());
        }
        List<DataSegment> segments = dataSourceMap.get(dataSource.getName());
        segments.addAll(dataSource.getSegments());
      }
    }
    return dataSourceMap;
  }

  @GET
  @Produces("application/json")
  public Iterable<String> getDataSources()
  {
    return getSegmentsForDatasources().keySet();
  }

  @GET
  @Path("/{dataSourceName}")
  @Produces("application/json")
  public Map<String, Object> getDatasource(
      @PathParam("dataSourceName") String dataSourceName,
      @QueryParam("interval") String interval,
      @QueryParam("full") String full
  )
  {
    if(full == null) {
      return ImmutableMap.<String, Object>of(
          KEY_DIMENSIONS, getDatasourceDimensions(dataSourceName, interval),
          KEY_METRICS, getDatasourceMetrics(dataSourceName, interval)
      );
    }

    Interval theInterval;
    if (interval == null || interval.isEmpty()) {
      DateTime now = new DateTime();
      theInterval = new Interval(now.minusMillis(SEGMENT_HISTORY_MILLIS), now);
    } else {
      theInterval = new Interval(interval);
    }

    VersionedIntervalTimeline<String, ServerSelector> timeline = timelineServerView.getTimeline(new TableDataSource(dataSourceName));
    List<TimelineObjectHolder<String, ServerSelector>> serversLookup = timeline != null ? timeline.lookup(theInterval) : null;
    if(serversLookup == null || serversLookup.isEmpty()) {
      return Collections.EMPTY_MAP;
    }
    Map<Interval,Object> servedIntervals = new TreeMap<>(new Comparator<Interval>()
    {
      @Override
      public int compare(Interval o1, Interval o2)
      {
        if(o1.equals(o2) || o1.overlaps(o2)) {
          return 0;
        } else {
          return o1.isBefore(o2) ? -1 : 1;
        }
      }
    });

    for (TimelineObjectHolder<String, ServerSelector> holder : serversLookup) {
      servedIntervals.put(holder.getInterval(), ImmutableMap.of(KEY_DIMENSIONS, Sets.newHashSet(), KEY_METRICS, Sets.newHashSet()));
    }

    List<DataSegment> segments = getSegmentsForDatasources().get(dataSourceName);
    if (segments == null || segments.isEmpty()) {
      //Note: is this check really required? when there are segments in the timeline, there
      //must be segments in the inventory as well.
      return Collections.EMPTY_MAP;
    }

    for (DataSegment segment : segments) {
      if(servedIntervals.containsKey(segment.getInterval())) {
        Map<String,Set<String>> columns = (Map<String,Set<String>>)servedIntervals.get(segment.getInterval());
        columns.get(KEY_DIMENSIONS).addAll(segment.getDimensions());
        columns.get(KEY_METRICS).addAll(segment.getMetrics());
      }
    }

    //collapse intervals if they abut and have same set of columns
    Map<String,Object> result = Maps.newLinkedHashMap();
    Interval curr = null;
    Map<String,Set<String>> cols = null;
    for(Map.Entry<Interval,Object> e : servedIntervals.entrySet()) {
      Interval ival = e.getKey();
      if(curr != null && curr.abuts(ival) && cols.equals(e.getValue())) {
        curr = curr.withEnd(ival.getEnd());
      } else {
        if(curr != null) {
          result.put(curr.toString(), cols);
        }
        curr = ival;
        cols = (Map<String,Set<String>>)e.getValue();
      }
    }
    //add the last one in
    if(curr != null) {
      result.put(curr.toString(), cols);
    }
    return result;
  }

  @GET
  @Path("/{dataSourceName}/dimensions")
  @Produces("application/json")
  public Iterable<String> getDatasourceDimensions(
      @PathParam("dataSourceName") String dataSourceName,
      @QueryParam("interval") String interval
  )
  {
    final List<DataSegment> segments = getSegmentsForDatasources().get(dataSourceName);
    final Set<String> dims = Sets.newHashSet();

    if (segments == null || segments.isEmpty()) {
      return dims;
    }

    Interval theInterval;
    if (interval == null || interval.isEmpty()) {
      DateTime now = new DateTime();
      theInterval = new Interval(now.minusMillis(SEGMENT_HISTORY_MILLIS), now);
    } else {
      theInterval = new Interval(interval);
    }

    for (DataSegment segment : segments) {
      if (theInterval.overlaps(segment.getInterval())) {
        dims.addAll(segment.getDimensions());
      }
    }

    return dims;
  }

  @GET
  @Path("/{dataSourceName}/metrics")
  @Produces("application/json")
  public Iterable<String> getDatasourceMetrics(
      @PathParam("dataSourceName") String dataSourceName,
      @QueryParam("interval") String interval
  )
  {
    final List<DataSegment> segments = getSegmentsForDatasources().get(dataSourceName);
    final Set<String> metrics = Sets.newHashSet();

    if (segments == null || segments.isEmpty()) {
      return metrics;
    }
    
    Interval theInterval;
    if (interval == null || interval.isEmpty()) {
      DateTime now = new DateTime();
      theInterval = new Interval(now.minusMillis(SEGMENT_HISTORY_MILLIS), now);
    } else {
      theInterval = new Interval(interval);
    }

    for (DataSegment segment : segments) {
      if (theInterval.overlaps(segment.getInterval())) {
        metrics.addAll(segment.getMetrics());
      }
    }

    return metrics;
  }
}
