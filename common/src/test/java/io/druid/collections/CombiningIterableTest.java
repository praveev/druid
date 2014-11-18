package io.druid.collections;

import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class CombiningIterableTest
{

  @Test
  public void testCreateSplatted() throws Exception
  {
    List<Integer> list1 = Arrays.asList(1, 2, 5, 7, 9);
    List<Integer> list2 = Arrays.asList(2, 8);
    List<Integer> list3 = Arrays.asList(4, 6, 8);
    final ArrayList<Iterable<Integer>> iterators = Lists.newArrayList();
    iterators.add(list1);
    iterators.add(list2);
    iterators.add(list3);
    HashMap<Integer,String> expectedMap = new HashMap<>();
    for (Iterable myIt : iterators) {
      Iterator it = myIt.iterator();
      while (it.hasNext()) {
        expectedMap.put((Integer) it.next(),"1");
      }
    }
    CombiningIterable<Integer> iter = CombiningIterable.createSplatted(
        iterators,
        Ordering.<Integer>natural()
    );

    Iterator<Integer> it = iter.iterator();

    while (it.hasNext()) {
      Integer key = it.next();
      Assert.assertTrue(expectedMap.containsKey(key));
      expectedMap.remove(key);
    }
    Assert.assertEquals("size does not match", 0, expectedMap.size());
  }
}