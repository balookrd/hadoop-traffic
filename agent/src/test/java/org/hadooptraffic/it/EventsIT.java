package org.hadooptraffic.it;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.net.NetUtils;
import org.hadooptraffic.model.Op;
import org.hadooptraffic.model.Role;
import org.junit.Test;

public class EventsIT extends ClusterTestBase {

  /**
   * Event sums equal JMX: the aggregator and the file see the same emissions. Polls until both
   * bytes and the number of final events match, since the last event may still be queued.
   */
  private static void assertMatchesJmx(String user, Op op, Role role) throws Exception {
    Await.until(() -> {
      try {
        List<JsonNode> ev = EventFiles.read();
        long ops = TrafficAssert.ops(user, op, role);
        return ops > 0
            && EventFiles.bytes(ev, user, op.label(), role.label())
                == TrafficAssert.bytes(user, op, role)
            && EventFiles.lastCount(ev, user, op.label(), role.label()) == ops;
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }, AWAIT_MS);
  }

  @Test
  public void eventsMatchJmxCounters() throws Exception {
    String writer = "erin-events";
    String reader = "frank-events";
    Path p = new Path(DIR, "events-file");
    as(writer, () -> {
      try (FileSystem fs = FileSystem.newInstance(cluster.getURI(), conf)) {
        DFSTestUtil.createFile(fs, p, FILE_LEN, (short) 3, 11L);
      }
      return null;
    });
    as(reader, () -> {
      try (FileSystem fs = FileSystem.newInstance(cluster.getURI(), conf)) {
        DFSTestUtil.readFile(fs, p);
      }
      return null;
    });
    Await.until(() -> TrafficAssert.bytes(writer, Op.WRITE, Role.CLIENT) == FILE_LEN
        && TrafficAssert.bytes(writer, Op.WRITE, Role.PIPELINE) == 2L * FILE_LEN
        && TrafficAssert.bytes(reader, Op.READ, Role.CLIENT) == FILE_LEN, AWAIT_MS);

    assertMatchesJmx(writer, Op.WRITE, Role.CLIENT);
    assertMatchesJmx(writer, Op.WRITE, Role.PIPELINE);
    assertMatchesJmx(reader, Op.READ, Role.CLIENT);
    assertEquals(3L, EventFiles.lastCount(EventFiles.read(), writer, "write", "client"));

    for (JsonNode e : EventFiles.read()) {
      if (e.get("user").asText().equals(writer)) {
        assertTrue(e.toString(), e.get("file_block_id").asLong() > 0);
        assertTrue(e.toString(), e.get("op_id").asText().matches("[0-9a-f]{6}-\\d+"));
      }
    }
  }

  @Test
  public void webHdfsEventsCarryThePath() throws Exception {
    String user = "gina-events";
    Path p = new Path(DIR, "gina-web");
    URI web = new URI("webhdfs://"
        + NetUtils.getHostPortString(cluster.getNameNode().getHttpAddress()));
    as(user, () -> {
      try (FileSystem fs = FileSystem.newInstance(web, conf)) {
        DFSTestUtil.createFile(fs, p, FILE_LEN, (short) 1, 12L);
        DFSTestUtil.readFile(fs, p);
      }
      return null;
    });
    Await.until(() -> TrafficAssert.bytes(user, Op.WEBHDFS_CREATE, Role.CLIENT) == FILE_LEN
        && TrafficAssert.bytes(user, Op.WEBHDFS_READ, Role.CLIENT) == FILE_LEN, AWAIT_MS);

    Set<String> expected = Collections.singleton(p.toString());
    Await.until(() -> {
      try {
        List<JsonNode> ev = EventFiles.read();
        return expected.equals(EventFiles.paths(ev, user, "webhdfs_create"))
            && expected.equals(EventFiles.paths(ev, user, "webhdfs_read"));
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }, AWAIT_MS);
  }
}
