package org.hadooptraffic.metrics;

import static org.junit.Assert.assertEquals;

import java.util.Map;
import org.hadooptraffic.aggregate.TrafficAggregator;
import org.hadooptraffic.aggregate.TrafficKey;
import org.hadooptraffic.model.Locality;
import org.hadooptraffic.model.Op;
import org.hadooptraffic.model.Role;
import org.hadooptraffic.model.TrafficEvent;
import org.junit.Test;

public class UserTrafficMetricsSourceTest {
  @Test
  public void attributeNameEncodesAllDimensions() {
    TrafficKey k = new TrafficKey("alice", Op.WEBHDFS_READ, Role.CLIENT, Locality.REMOTE);
    assertEquals("Bytes.user.alice.op.webhdfs_read.role.client.locality.remote",
        UserTrafficMetricsSource.attributeName("Bytes", k));
  }

  /** Hadoop's /jmx servlet silently skips attributes whose name contains '=', ':' or ' '. */
  @Test
  public void attributeNameAvoidsCharactersTheJmxServletSkips() {
    TrafficKey k = new TrafficKey("a b=c:d", Op.READ, Role.CLIENT, Locality.LOCAL);
    assertEquals("Ops.user.a_b_c_d.op.read.role.client.locality.local",
        UserTrafficMetricsSource.attributeName("Ops", k));
  }

  @Test
  public void exposesCounterTripletPerKey() {
    TrafficAggregator agg = new TrafficAggregator(10);
    agg.onEvent(new TrafficEvent("alice", Op.WRITE, Role.CLIENT, Locality.REMOTE,
        "bp", 1L, 100L, 0L, 1L, "p"));
    agg.onEvent(new TrafficEvent("bob", Op.SCR_FDS, Role.CLIENT, Locality.LOCAL,
        "bp", 2L, 0L, 512L, 1L, "p"));

    Map<String, Long> m = UserTrafficMetricsSource.metricValues(agg.snapshot());

    assertEquals(6, m.size());
    assertEquals(Long.valueOf(100L),
        m.get("Bytes.user.alice.op.write.role.client.locality.remote"));
    assertEquals(Long.valueOf(1L),
        m.get("Ops.user.alice.op.write.role.client.locality.remote"));
    assertEquals(Long.valueOf(0L),
        m.get("EstimatedBytes.user.alice.op.write.role.client.locality.remote"));
    assertEquals(Long.valueOf(512L),
        m.get("EstimatedBytes.user.bob.op.scr_fds.role.client.locality.local"));
  }
}
