package io.druid.collections;

import com.google.common.collect.PeekingIterator;
import com.metamx.common.guava.nary.BinaryFn;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.easymock.EasyMock;

import java.util.Comparator;
import java.util.NoSuchElementException;

public class CombiningIteratorTest
{
  private CombiningIterator <String> testingIterator;
  private Comparator <String> comparator;
  private BinaryFn fn;
  private PeekingIterator <String> it ;

  @Before
  public void setUp() throws Exception
  {
    it = EasyMock.createMock(PeekingIterator.class);
    comparator = EasyMock.createMock(Comparator.class);
    fn = EasyMock.createMock(BinaryFn.class);
    testingIterator=CombiningIterator.create(it,comparator,fn);
  }

  @After
  public void tearDown() throws Exception
  {
    testingIterator=null;
  }

  @Test
  public void testHasNext() throws Exception
  {
    boolean expected=true;
    EasyMock.expect(it.hasNext()).andReturn(expected);
    EasyMock.replay(it);
    boolean actual = testingIterator.hasNext();
    EasyMock.verify(it);
    Assert.assertEquals("The hasNext function is broken",expected,actual);
  }

  @Test
  public void testFalseBranchNext() throws Exception
  {
    boolean expected=true;
    EasyMock.expect(it.hasNext()).andReturn(expected);
    expected=false;
    EasyMock.expect(it.hasNext()).andReturn(expected);
    EasyMock.replay(it);
    Object res = testingIterator.next();
    EasyMock.verify(it);
    Assert.assertNull("Should be null",res);
  }

  @Test
  public void testNext() throws Exception
  {
    boolean expected=true;
    EasyMock.expect(it.hasNext()).andReturn(expected).times(4);
    String defaultString="S1";
    String resString="S2";
    EasyMock.expect(it.next()).andReturn(defaultString);
    EasyMock.expect(fn.apply(EasyMock.eq(defaultString), EasyMock.isNull())).andReturn(resString);
    EasyMock.expect(it.next()).andReturn(defaultString);
    EasyMock.expect(comparator.compare(EasyMock.eq(resString), EasyMock.eq(defaultString))).andReturn(0);
    EasyMock.expect(it.next()).andReturn(defaultString);
    EasyMock.expect(fn.apply(EasyMock.eq(resString), EasyMock.eq(defaultString))).andReturn(resString);
    EasyMock.expect(comparator.compare(EasyMock.eq(resString), EasyMock.eq(defaultString))).andReturn(1);

    EasyMock.replay(it);
    EasyMock.replay(fn);
    EasyMock.replay(comparator);

    String actual = testingIterator.next();
    Assert.assertEquals(resString,actual);

    EasyMock.verify(it);
    EasyMock.verify(comparator);
    EasyMock.verify(fn);
  }

  @Test(expected = NoSuchElementException.class)
  public void testExceptionInNext() throws Exception
  {
    boolean expected=false;
    EasyMock.expect(it.hasNext()).andReturn(expected);
    EasyMock.replay(it);
    testingIterator.next();
    EasyMock.verify(it);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testRemove() throws Exception
  {
    testingIterator.remove();
  }
}