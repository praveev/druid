/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.metadata;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.CacheStrategy;
import io.druid.query.TableDataSource;
import io.druid.query.metadata.metadata.ColumnAnalysis;
import io.druid.query.metadata.metadata.SegmentAnalysis;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.query.spec.QuerySegmentSpecs;
import io.druid.segment.column.ValueType;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

public class SegmentMetadataQueryQueryToolChestTest
{
  @Test
  public void testCacheStrategy() throws Exception
  {
    SegmentMetadataQuery query = new SegmentMetadataQuery(
      new TableDataSource("dummy"),
      QuerySegmentSpecs.create("2015-01-01/2015-01-02"),
      null,
      null,
      null,
      null,
      false
    );

    CacheStrategy<SegmentAnalysis, SegmentAnalysis, SegmentMetadataQuery> strategy =
        new SegmentMetadataQueryQueryToolChest(null).getCacheStrategy(query);

    // Test cache key generation
    byte[] expectedKey = {0x04, 0x01, (byte) 0xFF, 0x00, 0x01, 0x02};
    byte[] actualKey = strategy.computeCacheKey(query);
    Assert.assertArrayEquals(expectedKey, actualKey);

    SegmentAnalysis result = new SegmentAnalysis(
        "testSegment",
        ImmutableList.of(
            new Interval("2011-01-12T00:00:00.000Z/2011-04-15T00:00:00.001Z")
        ),
        ImmutableMap.of(
            "placement",
            new ColumnAnalysis(
                ValueType.STRING.toString(),
                true,
                10881,
                1,
                null
            )
        ), 71982,
        100
    );

    Object preparedValue = strategy.prepareForCache().apply(result);

    ObjectMapper objectMapper = new DefaultObjectMapper();
    SegmentAnalysis fromCacheValue = objectMapper.readValue(
        objectMapper.writeValueAsBytes(preparedValue),
        strategy.getCacheObjectClazz()
    );

    SegmentAnalysis fromCacheResult = strategy.pullFromCache().apply(fromCacheValue);

    Assert.assertEquals(result, fromCacheResult);
  }
}
