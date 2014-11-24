package io.druid.indexer;

import com.google.common.primitives.Bytes;

import org.hamcrest.number.OrderingComparison;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.metamx.common.Pair;

import java.nio.ByteBuffer;

public class BucketTest
{
  Bucket bucket;
  int shardNum;
  int partitionNum;
  DateTime time;

  @Before public void setUp()
  {
    time = new DateTime(2014, 11, 24, 10, 30);
    shardNum = 1;
    partitionNum = 1;
    bucket = new Bucket(shardNum, time, partitionNum);
  }

  @After public void tearDown()
  {
    bucket = null;
  }

  @Test public void testToGroupKey()
  {
    byte[] firstPart = {1, 1, 0, 10};
    byte[] secondPart = {2, 4, 0, 5};
    byte[] actualGroupParts = bucket.toGroupKey(firstPart,secondPart);
    ByteBuffer preambleBuffer = ByteBuffer.allocate(Bucket.PREAMBLE_BYTES);
    preambleBuffer.putInt(shardNum);
    preambleBuffer.putLong(time.getMillis());
    preambleBuffer.putInt(partitionNum);
    byte[] expectedGroupParts = Bytes.concat(preambleBuffer.array(),firstPart,secondPart);
    Assert.assertArrayEquals(expectedGroupParts, actualGroupParts);
  }

  @Test public void testToString()
  {
    String expectedString = "Bucket{" +
        "time=" + time +
        ", partitionNum=" + partitionNum +
        ", shardNum=" + shardNum +
        '}';
    Assert.assertEquals(bucket.toString(),expectedString);
  }

  @Test public void testEquals()
  {
    Assert.assertFalse("Object should not be equals to NULL", bucket.equals(null));
    Assert.assertFalse("Objects do not have the same Class",bucket.equals(new Integer(0)));
    Assert.assertFalse("Objects do not have the same partitionNum",
        bucket.equals(new Bucket(shardNum, time, partitionNum + 1)));
    Assert.assertFalse("Objects do not have the same shardNum",
        bucket.equals(new Bucket(shardNum + 1,time,partitionNum)));
    Assert.assertFalse("Objects do not have the same time",bucket.equals(new Bucket(shardNum,new DateTime(),partitionNum)));
    Assert.assertFalse("Object do have NULL time",bucket.equals(new Bucket(shardNum,null,partitionNum)));
    Assert.assertTrue("Objects must be the same",bucket.equals(new Bucket(shardNum, time, partitionNum)));

  }

  @Test public void testHashCode()
  {
    int hashCode = bucket.hashCode();
    Assert.assertThat(hashCode, OrderingComparison.greaterThanOrEqualTo(31 * partitionNum + shardNum));
    bucket = new Bucket(shardNum,null,partitionNum);
    hashCode = bucket.hashCode();
    Assert.assertEquals(hashCode, (31 * partitionNum + shardNum));
  }

  @Test public void testFromGroupKey()
  {
    byte[] leftArray = {1, 0, 9, 8, 1};
    ByteBuffer preambleBuffer = ByteBuffer.allocate(Bucket.PREAMBLE_BYTES);
    preambleBuffer.putInt(shardNum);
    preambleBuffer.putLong(time.getMillis());
    preambleBuffer.putInt(partitionNum);
    byte keyBytes[] = Bytes.concat(preambleBuffer.array(),leftArray);
    Pair<Bucket, byte[]> actualPair = bucket.fromGroupKey(keyBytes);
    Assert.assertEquals("Bucket is not matching", bucket,actualPair.lhs);
    Assert.assertArrayEquals("Left Array is not matching", leftArray, actualPair.rhs);
  }
}