package io.druid.server.http;

import io.druid.server.coordinator.DruidCoordinator;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URL;

public class CoordinatorRedirectInfoTest
{
  private DruidCoordinator druidCoordinator;
  private CoordinatorRedirectInfo coordinatorRedirectInfo;

  @Before
  public void setUp()
  {
    druidCoordinator = EasyMock.createMock(DruidCoordinator.class);
    coordinatorRedirectInfo = new CoordinatorRedirectInfo(druidCoordinator);
  }

  @Test
  public void testDoLocal()
  {
    EasyMock.expect(druidCoordinator.isLeader()).andReturn(true).anyTimes();
    EasyMock.replay(druidCoordinator);
    Assert.assertTrue(coordinatorRedirectInfo.doLocal());
    EasyMock.verify(druidCoordinator);
  }

  @Test
  public void testGetRedirectURLNull()
  {
    EasyMock.expect(druidCoordinator.getCurrentLeader()).andReturn(null).anyTimes();
    EasyMock.replay(druidCoordinator);
    URL url = coordinatorRedirectInfo.getRedirectURL("query", "request");
    Assert.assertNull(url);
  }
  @Test
  public void testGetRedirectURL()
  {
    String host = "localhost";
    String query = "query";
    String request = "request";
    EasyMock.expect(druidCoordinator.getCurrentLeader()).andReturn(host).anyTimes();
    EasyMock.replay(druidCoordinator);
    URL url = coordinatorRedirectInfo.getRedirectURL(query,request);
    Assert.assertTrue(url.toString().contains(host+request+"?"+query));
  }
}