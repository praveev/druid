package io.druid.collections;

import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Iterables;

import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;


import java.util.*;

public class CombiningIterableTest
{
  @Test
  public void testCreateSplatted()
  {
    List<Integer> firstList = Arrays.asList(1, 2, 5, 7, 9, 10, 20);
    List<Integer> secondList = Arrays.asList(1, 2, 5, 8, 9);
    Set<Integer> mergedLists = new HashSet<>();
    mergedLists.addAll(firstList);
    mergedLists.addAll(secondList);
    ArrayList<Iterable<Integer>> iterators = Lists.newArrayList();
    iterators.add(firstList);
    iterators.add(secondList);
    CombiningIterable<Integer> actualIterable = CombiningIterable.createSplatted(
        iterators,
        Ordering.<Integer>natural()
    );
    Assert.assertEquals(mergedLists.size(),Iterables.size(actualIterable));
    Set actualHashset = Sets.newHashSet(actualIterable);
    Assert.assertEquals(actualHashset,mergedLists);
  }
}