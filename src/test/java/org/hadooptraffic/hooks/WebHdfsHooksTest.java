package org.hadooptraffic.hooks;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.hadooptraffic.model.Locality;
import org.hadooptraffic.model.Op;
import org.hadooptraffic.model.Role;
import org.hadooptraffic.model.TrafficEvent;
import org.hadooptraffic.sink.TrafficSinks;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class WebHdfsHooksTest {
  private final List<TrafficEvent> events = new ArrayList<>();
  private final UserGroupInformation erin = UserGroupInformation.createRemoteUser("erin");

  @Before
  public void setUp() {
    TrafficSinks.clear();
    TrafficSinks.register(events::add);
  }

  @After
  public void tearDown() {
    TrafficSinks.clear();
  }

  @Test
  public void boundWriterAccumulatesAndFinishesOnce() {
    Object writer = new Object();
    boolean started = WebHdfsHooks.onRequestEnter("onCreate", erin, null);
    WebHdfsHooks.bindPending(writer);
    WebHdfsHooks.onRequestExit(started);

    WebHdfsHooks.addBytes(writer, 100);
    WebHdfsHooks.addBytes(writer, 23);
    WebHdfsHooks.finish(writer);
    WebHdfsHooks.finish(writer);

    assertEquals(1, events.size());
    TrafficEvent e = events.get(0);
    assertEquals("erin", e.getUser());
    assertEquals(Op.WEBHDFS_CREATE, e.getOp());
    assertEquals(Role.CLIENT, e.getRole());
    assertEquals(Locality.REMOTE, e.getLocality());
    assertEquals(123L, e.getBytes());
    assertEquals(-1L, e.getBlockId());
  }

  @Test
  public void requestWithoutBoundStreamEmitsNothing() {
    boolean started = WebHdfsHooks.onRequestEnter("onOpen", erin, null);
    WebHdfsHooks.onRequestExit(started);

    assertTrue(started);
    assertTrue(events.isEmpty());
  }

  @Test
  public void unknownMethodIsIgnored() {
    Object owner = new Object();
    boolean started = WebHdfsHooks.onRequestEnter("onGetFileChecksum", erin, null);
    WebHdfsHooks.bindPending(owner);
    WebHdfsHooks.onRequestExit(started);
    WebHdfsHooks.finish(owner);

    assertTrue(events.isEmpty());
  }

  @Test
  public void tagsClientConfOnlyInsideRequest() {
    Configuration conf = new Configuration(false);
    assertSame(conf, WebHdfsHooks.tagClientConf(conf));

    boolean started = WebHdfsHooks.onRequestEnter("onOpen", erin, null);
    Configuration tagged = WebHdfsHooks.tagClientConf(conf);
    WebHdfsHooks.onRequestExit(started);

    assertNotSame(conf, tagged);
    assertEquals(WebHdfsHooks.TASK_ID, tagged.get("mapreduce.task.attempt.id"));
    assertNull(conf.get("mapreduce.task.attempt.id"));
  }
}
