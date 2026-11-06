package org.hadooptraffic.aggregate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import org.hadooptraffic.model.Locality;
import org.hadooptraffic.model.Op;
import org.hadooptraffic.model.Role;
import org.hadooptraffic.model.TrafficEvent;
import org.junit.Test;

public class TrafficAggregatorTest {
  private static TrafficEvent ev(String user, Op op, Role role, long bytes, long est) {
    return new TrafficEvent(user, op, role, Locality.LOCAL, "bp-1", 1L, bytes, est, 1L, "p");
  }

  private static TrafficKey key(String user, Op op, Role role) {
    return new TrafficKey(user, op, role, Locality.LOCAL);
  }

  @Test
  public void sumsEventsWithSameKey() {
    TrafficAggregator agg = new TrafficAggregator(10);
    agg.onEvent(ev("alice", Op.READ, Role.CLIENT, 100, 0));
    agg.onEvent(ev("alice", Op.READ, Role.CLIENT, 50, 0));

    TrafficCounters.Snapshot s = agg.snapshot().get(key("alice", Op.READ, Role.CLIENT));
    assertEquals(150L, s.getBytes());
    assertEquals(2L, s.getOps());
    assertEquals(0L, s.getEstimatedBytes());
  }

  @Test
  public void separatesKeysByEveryDimension() {
    TrafficAggregator agg = new TrafficAggregator(10);
    agg.onEvent(ev("alice", Op.WRITE, Role.CLIENT, 10, 0));
    agg.onEvent(ev("alice", Op.WRITE, Role.PIPELINE, 20, 0));
    agg.onEvent(ev("bob", Op.WRITE, Role.CLIENT, 30, 0));
    agg.onEvent(new TrafficEvent("alice", Op.WRITE, Role.CLIENT, Locality.REMOTE,
        "bp-1", 1L, 40, 0, 1L, "p"));

    Map<TrafficKey, TrafficCounters.Snapshot> snap = agg.snapshot();
    assertEquals(4, snap.size());
    assertEquals(20L, snap.get(key("alice", Op.WRITE, Role.PIPELINE)).getBytes());
    assertEquals(40L, snap.get(new TrafficKey("alice", Op.WRITE, Role.CLIENT,
        Locality.REMOTE)).getBytes());
  }

  @Test
  public void countsEstimatedBytes() {
    TrafficAggregator agg = new TrafficAggregator(10);
    agg.onEvent(ev("alice", Op.SCR_FDS, Role.CLIENT, 0, 4096));

    TrafficCounters.Snapshot s = agg.snapshot().get(key("alice", Op.SCR_FDS, Role.CLIENT));
    assertEquals(0L, s.getBytes());
    assertEquals(4096L, s.getEstimatedBytes());
    assertEquals(1L, s.getOps());
  }

  @Test
  public void capsDistinctUsers() {
    TrafficAggregator agg = new TrafficAggregator(2);
    agg.onEvent(ev("alice", Op.READ, Role.CLIENT, 1, 0));
    agg.onEvent(ev("bob", Op.READ, Role.CLIENT, 1, 0));
    agg.onEvent(ev("carol", Op.READ, Role.CLIENT, 7, 0));
    agg.onEvent(ev("alice", Op.READ, Role.CLIENT, 1, 0));

    Map<TrafficKey, TrafficCounters.Snapshot> snap = agg.snapshot();
    assertFalse(snap.containsKey(key("carol", Op.READ, Role.CLIENT)));
    assertEquals(7L, snap.get(key(TrafficAggregator.OTHER_USER, Op.READ, Role.CLIENT)).getBytes());
    assertEquals(2L, snap.get(key("alice", Op.READ, Role.CLIENT)).getBytes());
  }

  @Test
  public void snapshotIsDetachedFromLaterEvents() {
    TrafficAggregator agg = new TrafficAggregator(10);
    agg.onEvent(ev("alice", Op.READ, Role.CLIENT, 1, 0));
    Map<TrafficKey, TrafficCounters.Snapshot> before = agg.snapshot();
    agg.onEvent(ev("alice", Op.READ, Role.CLIENT, 1, 0));

    assertEquals(1L, before.get(key("alice", Op.READ, Role.CLIENT)).getBytes());
    assertTrue(agg.snapshot().get(key("alice", Op.READ, Role.CLIENT)).getBytes() == 2L);
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsNonPositiveCap() {
    new TrafficAggregator(0);
  }
}
