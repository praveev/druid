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

package io.druid.indexer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.util.Lists;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.metamx.common.Granularity;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.impl.JSONDataSpec;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularity;
import io.druid.indexer.partitions.PartitionsSpec;
import io.druid.indexer.rollup.DataRollupSpec;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.indexing.granularity.GranularitySpec;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

/**
 */
public class HadoopDruidIndexerConfigTest
{
  private static final ObjectMapper jsonMapper = new DefaultObjectMapper();

  public static <T> T jsonReadWriteRead(String s, Class<T> klass)
  {
    try {
      return jsonMapper.readValue(jsonMapper.writeValueAsBytes(jsonMapper.readValue(s, klass)), klass);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testFromFile() throws IOException
  {
    final String DATA_SOURCE = "data_source";
    final boolean OVER_WRITE_FILE = true;
    final boolean IGNORE_INVALID_ROWS = true;
    final long MAX_PARTITION_SIZE = 100;
    final long TARGET_PARTITION_SIZE = 111;
    final boolean UPDATER_JOB_SPEC = false; //it can be only false
    final boolean COMBINE_TEXT = true;
    final boolean IS_DETERMINING_PARTITIONS = true;
    String schemaDescription =  "{"
        + "\"schema\":{"
        + "\"dataSource\": \"" + DATA_SOURCE + "\","
        + "\"overwriteFiles\": \"" + OVER_WRITE_FILE + "\","
        + "\"combineText\": \" " + COMBINE_TEXT + "\","
        + "\"ignoreInvalidRows\": \" " + IGNORE_INVALID_ROWS + "\","
        + "\"partitionsSpec\":{"
          + "\"maxPartitionSize\": \" " + MAX_PARTITION_SIZE + "\","
          + "\"targetPartitionSize\":\" " + TARGET_PARTITION_SIZE + "\","
          + "\"isDeterminingPartitions\": \"" + IS_DETERMINING_PARTITIONS + "\""
        + "  }"
        + "}"
      + "}";
    File tmpFile = tmpFolder.newFile();
    Files.write(schemaDescription.getBytes(StandardCharsets.UTF_8), tmpFile);
    HadoopDruidIndexerConfig hadoopDruidIndexerConfig = HadoopDruidIndexerConfig.fromFile(tmpFile);
    Assert.assertEquals("getDataSource",DATA_SOURCE,hadoopDruidIndexerConfig.getDataSource());
    Assert.assertEquals("isIgnoreInvalidRows",OVER_WRITE_FILE, hadoopDruidIndexerConfig.isOverwriteFiles());
    Assert.assertEquals("isIgnoreInvalidRows", IGNORE_INVALID_ROWS, hadoopDruidIndexerConfig.isIgnoreInvalidRows());
    Assert.assertTrue("isDeterminingPartitions Should be true", hadoopDruidIndexerConfig.isDeterminingPartitions());
    Assert.assertTrue("Shoudl be member of PartitionsSpec",
        hadoopDruidIndexerConfig.getPartitionsSpec() instanceof PartitionsSpec);
    Assert.assertEquals("getMaxPartitionSize", MAX_PARTITION_SIZE, hadoopDruidIndexerConfig.getMaxPartitionSize());
    Assert.assertEquals("getTargetPartitionSize", TARGET_PARTITION_SIZE,
        (long) hadoopDruidIndexerConfig.getTargetPartitionSize());
    Assert.assertEquals("isUpdaterJobSpecSet", UPDATER_JOB_SPEC, hadoopDruidIndexerConfig.isUpdaterJobSpecSet());
    Assert.assertEquals("isCombineText", COMBINE_TEXT, hadoopDruidIndexerConfig.isCombineText());
    tmpFolder.delete();
  }

  @Test
  public void testFromConfig()
  {
    Configuration conf = new Configuration();
    final String DATA_SOURCE = "the_data_source_for_test_from_config";
    final String TIMES_STAMP_COLUMN = "timestamp";

    String schemaSpec =  "{"
        + "\"dataSource\": \"" + DATA_SOURCE + "\","
        + "\"version\": \"" + new DateTime().toString() + "\","
        + "\"parser\":{\"type\":\"map\"},"
        + "\"workingPath\": \"\\tmp\","
        + "\"dataSpec\" : {\"format\": \"json\"},"
        + "\"timestampSpec\":{\"column\":\"" + TIMES_STAMP_COLUMN + "\", \"format\":\"auto\"},"
        + "\"segmentOutputPath\": \"hdfs://server:9100/tmp/druid/datatest\","
        + "\"pathSpec\":{\"type\" : \"static\"}"
        + "}";
    conf.set(HadoopDruidIndexerConfig.CONFIG_PROPERTY,schemaSpec);
    HadoopDruidIndexerConfig cfg = HadoopDruidIndexerConfig.fromConfiguration(conf);
    Assert.assertEquals("Data source is not matching",DATA_SOURCE,cfg.getDataSource());
    Assert.assertEquals("TimestampColumn is not matching",
        TIMES_STAMP_COLUMN,cfg.getParser().getParseSpec().getTimestampSpec().getTimestampColumn());
  }

  @Test
  public void testMakePath()
  {

    String dataSource = "source";
    Interval interval = new Interval(new DateTime(), new DateTime().plus(1));
    String version = new DateTime().toString();
    DataSegment segment = new DataSegment(
        dataSource,
        interval,
        version,
        null,
        null,
        null,
        null,
        0,
        0
    );

    HadoopIngestionSpec schema;

    try {
      schema = jsonReadWriteRead(
          "{"
              + "\"dataSource\": \"" + dataSource + "\""
              + "}",
          HadoopIngestionSpec.class
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    HadoopDruidIndexerConfig configuration = new HadoopDruidIndexerConfig(
        schema.withTuningConfig(
            schema.getTuningConfig()
                .withVersion(
                    new DateTime().toString()
                )
        )
    );

    String expectedIntermediatePath = String.format("%s/%s/%s",
        schema.getTuningConfig().getWorkingPath(),
        schema.getDataSchema().getDataSource(),
        configuration.getSchema().getTuningConfig().getVersion().replace(":", ""));

    String expectedDescriptorInfoPath = String.format("%s/%s/%s.json",
        expectedIntermediatePath,
        "segmentDescriptorInfo",
        segment.getIdentifier().replace(":", ""));
    Path actualDescriptorInfoPath = configuration.makeDescriptorInfoPath(segment);
    Assert.assertEquals(expectedDescriptorInfoPath, actualDescriptorInfoPath.toString());

    String expectedGroupedDataDir = String.format("%s/%s",
      expectedIntermediatePath,
      "groupedData");
    Path actualGroupedDataDir = configuration.makeGroupedDataDir();
    Assert.assertEquals(expectedGroupedDataDir, actualGroupedDataDir.toString());

    String expectedIntervalInfoPath = String.format("%s/%s",
        expectedIntermediatePath,
        "intervals.json");
    Path actualIntervalInfoPath = configuration.makeIntervalInfoPath();
    Assert.assertEquals(expectedIntervalInfoPath, actualIntervalInfoPath.toString());

    String expectedSegmentPartitionInfoPath = String.format("%s/%s_%s/%s",
        expectedIntermediatePath,
        ISODateTimeFormat.basicDateTime().print(interval.getStart()),
        ISODateTimeFormat.basicDateTime().print(interval.getEnd()),
        "partitions.json"
        );
    Path actualSegmentPartitionInfoPath = configuration.makeSegmentPartitionInfoPath(interval);
    Assert.assertEquals(expectedSegmentPartitionInfoPath, actualSegmentPartitionInfoPath.toString());
  }

  @Test
  public void testSetGetVersionSharedSpecs()
  {
    String version = new DateTime().toString();
    String dataSource = "source";
    HadoopIngestionSpec schema;
    DateTime dateTimeNow = new DateTime();

    try {
      schema = jsonReadWriteRead(
          "{"
              + "\"dataSource\": \"" + dataSource + "\""
              + "}",
          HadoopIngestionSpec.class
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    HadoopDruidIndexerConfig configuration = new HadoopDruidIndexerConfig(
        schema.withTuningConfig(
            schema.getTuningConfig()
                .withVersion(
                    new DateTime().toString()
                )
        )
    );
    configuration.setVersion(version);

    Assert.assertEquals(configuration.getSchema().getTuningConfig().getVersion(),version);

    List<HadoopyShardSpec> listHadoopyShardSpec = new ArrayList<>();
    listHadoopyShardSpec.add(new HadoopyShardSpec(null,1));
    Map<DateTime,List<HadoopyShardSpec>> shardSpecs = new HashMap<>();
    shardSpecs.put(dateTimeNow,listHadoopyShardSpec);
    configuration.setShardSpecs(shardSpecs);
    HadoopyShardSpec actualHadoopyShardSpec = configuration.getShardSpec(new Bucket(0, dateTimeNow, 0));
    Assert.assertTrue(listHadoopyShardSpec.contains(actualHadoopyShardSpec));
  }

  @Test
  public void testSetGetGranularitySpec()
  {
    Interval actualInterval = new Interval("2010-01-01/P1D");
    GranularitySpec granularitySpec = new UniformGranularitySpec(Granularity.DAY,
        QueryGranularity.DAY,
        ImmutableList.of(actualInterval),
        Granularity.DAY
    );

    HadoopIngestionSpec schema;

    try {
      schema = jsonReadWriteRead(
          "{"
              + "\"dataSource\": \"datasource\""
              + "}",
          HadoopIngestionSpec.class
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    HadoopDruidIndexerConfig configuration = new HadoopDruidIndexerConfig(
        schema.withTuningConfig(
            schema.getTuningConfig()
                .withVersion(
                    new DateTime().toString()
                )
        )
    );

    configuration.setGranularitySpec(granularitySpec);
    Assert.assertEquals(granularitySpec, configuration.getGranularitySpec());

    //hasNext is false since shardSpecs is Null
    Assert.assertFalse(configuration.getAllBuckets().get().iterator().hasNext());

    // Setting shardSpecs
    List<HadoopyShardSpec> listHadoopyShardSpec = ImmutableList.of(new HadoopyShardSpec(null,1));
    Map<DateTime,List<HadoopyShardSpec>> shardSpecs = new HashMap<>();
    shardSpecs.put(actualInterval.getStart() ,listHadoopyShardSpec);
    configuration.setShardSpecs(shardSpecs);
    Assert.assertTrue(actualInterval.contains(configuration.getAllBuckets().get().iterator().next().time));
    Assert.assertEquals(true, configuration.getIntervals().get().iterator().next().contains(actualInterval.getStart()));

    //test the case when there is no Intervals
    granularitySpec = new UniformGranularitySpec(Granularity.DAY,
        QueryGranularity.DAY,
        null,
        Granularity.DAY
    );
    configuration.setGranularitySpec(granularitySpec);
    Assert.assertEquals(Optional.absent(), configuration.getIntervals());
    Assert.assertEquals(Optional.absent(), configuration.getAllBuckets());
  }

  @Test
  public void testAddInputPaths() throws IOException
  {
    HadoopIngestionSpec schema;
    String pathName = "pathName";

    try {
      schema = jsonReadWriteRead(
          "{"
              + "\"dataSource\": \"datasource\","
              + "\"pathSpec\":{\"type\" : \"static\",\"paths\":\"" + pathName + "\"}"
              + "}",
          HadoopIngestionSpec.class
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    HadoopDruidIndexerConfig configuration = new HadoopDruidIndexerConfig(
        schema.withTuningConfig(
            schema.getTuningConfig()
                .withVersion(
                    new DateTime().toString()
                )
        )
    );

    Job job = new Job();
    configuration.addInputPaths(job);
    Configuration conf = job.getConfiguration();
    String actualPath = conf.get("mapreduce.input.fileinputformat.inputdir");
    Assert.assertTrue(actualPath.contains(pathName));
  }

  @Test
  public void testAddJobProperties() throws IOException
  {
    Job job = new Job();
    HadoopIngestionSpec schema;
    String entryKey = "keyToSet";
    String entryValue = "valueToSet";

    try {
      schema = jsonReadWriteRead(
          "{"
              + "\"dataSource\": \"datasource\","
              + "\"jobProperties\":{ \"" + entryKey + "\": \"" + entryValue+ "\"}"
              + "}",
          HadoopIngestionSpec.class
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    HadoopDruidIndexerConfig configuration = new HadoopDruidIndexerConfig(
        schema.withTuningConfig(
            schema.getTuningConfig()
                .withVersion(
                    new DateTime().toString()
                )
        )
    );
    configuration.addJobProperties(job);
    Assert.assertEquals(job.getConfiguration().get(entryKey), entryValue);

  }

  @Test
  public void testIntoConfiguration() throws IOException
  {
    Job job = new Job();
    String actualConfiguration =  "{"
        + "\"dataSource\": \"datasource\""
        + "}";
    HadoopIngestionSpec schema;

    try {
      schema = jsonReadWriteRead(actualConfiguration
         ,
          HadoopIngestionSpec.class
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    HadoopDruidIndexerConfig configuration = new HadoopDruidIndexerConfig(
        schema.withTuningConfig(
            schema.getTuningConfig()
                .withVersion(
                    new DateTime().toString()
                )
        )
    );
    configuration.intoConfiguration(job);
    Assert.assertEquals(job.getConfiguration().get(HadoopDruidIndexerConfig.CONFIG_PROPERTY),
        HadoopDruidIndexerConfig.jsonMapper.writeValueAsString(configuration));
  }

  @Test
  public void shouldMakeHDFSCompliantSegmentOutputPath()
  {
    HadoopIngestionSpec spec;

    try {
      spec = jsonReadWriteRead(
          "{"
          + "\"dataSource\": \"source\","
          + " \"granularitySpec\":{"
          + "   \"type\":\"uniform\","
          + "   \"gran\":\"hour\","
          + "   \"intervals\":[\"2012-07-10/P1D\"]"
          + " },"
          + "\"segmentOutputPath\": \"hdfs://server:9100/tmp/druid/datatest\""
          + "}",
          HadoopIngestionSpec.class
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    HadoopDruidIndexerConfig cfg = new HadoopDruidIndexerConfig(
        spec.withTuningConfig(
            spec.getTuningConfig()
                  .withVersion(
                      "some:brand:new:version"
                  )
        ),
        null
    );

    Bucket bucket = new Bucket(4711, new DateTime(2012, 07, 10, 5, 30), 4712);
    Path path = cfg.makeSegmentOutputPath(new DistributedFileSystem(), bucket);
    Assert.assertEquals(
        "hdfs://server:9100/tmp/druid/datatest/source/20120710T050000.000Z_20120710T060000.000Z/some_brand_new_version/4712",
        path.toString()
    );
  }

  @Test
  public void shouldMakeDefaultSegmentOutputPathIfNotHDFS()
  {
    final HadoopIngestionSpec spec;

    try {
      spec = jsonReadWriteRead(
          "{"
          + "\"dataSource\": \"the:data:source\","
          + " \"granularitySpec\":{"
          + "   \"type\":\"uniform\","
          + "   \"gran\":\"hour\","
          + "   \"intervals\":[\"2012-07-10/P1D\"]"
          + " },"
          + "\"segmentOutputPath\": \"/tmp/dru:id/data:test\""
          + "}",
          HadoopIngestionSpec.class
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    HadoopDruidIndexerConfig cfg = new HadoopDruidIndexerConfig(
        spec.withTuningConfig(
            spec.getTuningConfig()
                  .withVersion(
                      "some:brand:new:version"
                  )
        ),
        null
    );

    Bucket bucket = new Bucket(4711, new DateTime(2012, 07, 10, 5, 30), 4712);
    Path path = cfg.makeSegmentOutputPath(new LocalFileSystem(), bucket);
    Assert.assertEquals(
        "/tmp/dru:id/data:test/the:data:source/2012-07-10T05:00:00.000Z_2012-07-10T06:00:00.000Z/some:brand:new:version/4712",
        path.toString()
    );

  }

  @Test
  public void testHashedBucketSelection() {
    List<HadoopyShardSpec> specs = Lists.newArrayList();
    final int partitionCount = 10;
    for (int i = 0; i < partitionCount; i++) {
      specs.add(new HadoopyShardSpec(new HashBasedNumberedShardSpec(i, partitionCount, new DefaultObjectMapper()), i));
    }
    HadoopIngestionSpec spec = new HadoopIngestionSpec(
        null, null, null,
        "foo",
        new TimestampSpec("timestamp", "auto"),
        new JSONDataSpec(ImmutableList.of("foo"), null),
        new UniformGranularitySpec(
            Granularity.HOUR,
            QueryGranularity.MINUTE,
            ImmutableList.of(new Interval("2010-01-01/P1D")),
            Granularity.HOUR
        ),
        null,
        null,
        null,
        null,
        null,
        false,
        true,
        ImmutableMap.of(new DateTime("2010-01-01T01:00:00"), specs),
        false,
        new DataRollupSpec(ImmutableList.<AggregatorFactory>of(), QueryGranularity.MINUTE),
        null,
        false,
        ImmutableMap.of("foo", "bar"),
        false,
        null,
        null,
        null,
        null,
        null,
        null
    );
    HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromSpec(spec);
    final List<String> dims = Arrays.asList("diM1", "dIM2");
    final ImmutableMap<String, Object> values = ImmutableMap.<String, Object>of(
        "Dim1",
        "1",
        "DiM2",
        "2",
        "dim1",
        "3",
        "dim2",
        "4"
    );
    final long timestamp = new DateTime("2010-01-01T01:00:01").getMillis();
    final Bucket expectedBucket = config.getBucket(new MapBasedInputRow(timestamp, dims, values)).get();
    final long nextBucketTimestamp = QueryGranularity.MINUTE.next(QueryGranularity.MINUTE.truncate(timestamp));
    // check that all rows having same set of dims and truncated timestamp hash to same bucket
    for (int i = 0; timestamp + i < nextBucketTimestamp; i++) {
      Assert.assertEquals(
          expectedBucket.partitionNum,
          config.getBucket(new MapBasedInputRow(timestamp + i, dims, values)).get().partitionNum
      );
    }

  }
}
