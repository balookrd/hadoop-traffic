package org.hadooptraffic.context;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.hadooptraffic.model.Locality;
import org.hadooptraffic.model.Op;
import org.hadooptraffic.model.Role;
import org.hadooptraffic.model.TrafficEvent;
import org.hadooptraffic.sink.TrafficSinks;
import org.hadooptraffic.testutil.FakeClock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class OpContextTest {
  private static final long T0 = 1_762_430_000_000L;
  private final List<TrafficEvent> events = new ArrayList<>();
  private final FakeClock clock = new FakeClock(T0);

  @Before
  public void setUp() {
    TrafficSinks.clear();
    TrafficSinks.register(events::add);
    OpContext.setClock(clock);
    OpContext.setFlushIntervalSec(10);
  }

  @After
  public void tearDown() {
    TrafficSinks.clear();
    OpContext.setClock(Clock.SYSTEM);
    OpContext.setFlushIntervalSec(0);
  }

  private static OpContext ctx() {
    return new OpContext("alice", Op.READ, Role.CLIENT, Locality.REMOTE, "bp", 7L, "peer");
  }

  @Test
  public void shortOperationEmitsOneFinalEvent() {
    OpContext c = ctx();
    c.addBytes(100);
    clock.advanceSec(3);
    c.addBytes(50);
    c.finish();

    assertEquals(1, events.size());
    TrafficEvent e = events.get(0);
    assertEquals(150L, e.getBytes());
    assertTrue(e.isLast());
    assertEquals(3000L, e.getDurationMs());
    assertEquals(T0, e.getStartMillis());
    assertEquals(T0 + 3000L, e.getTimestampMillis());
    assertEquals(c.opId(), e.getOpId());
  }

  @Test
  public void longOperationEmitsDeltasThenRemainder() {
    OpContext c = ctx();
    c.addBytes(100);
    clock.advanceSec(10);
    c.addBytes(20);          // 10 s elapsed: delta of 120
    clock.advanceSec(5);
    c.addBytes(30);          // 5 s since last emission: nothing
    clock.advanceSec(6);
    c.addBytes(5);           // 11 s: delta of 35
    c.finish();              // remainder 0, last

    assertEquals(3, events.size());
    assertEquals(120L, events.get(0).getBytes());
    assertFalse(events.get(0).isLast());
    assertEquals(35L, events.get(1).getBytes());
    assertFalse(events.get(1).isLast());
    assertEquals(0L, events.get(2).getBytes());
    assertTrue(events.get(2).isLast());
    long sum = 0;
    for (TrafficEvent e : events) {
      sum += e.getBytes();
      assertEquals(c.opId(), e.getOpId());
      assertEquals(T0, e.getStartMillis());
    }
    assertEquals(155L, sum);
  }

  @Test
  public void noDeltasWhenDisabled() {
    OpContext.setFlushIntervalSec(0);
    OpContext c = ctx();
    c.addBytes(1);
    clock.advanceSec(60);
    c.addBytes(1);
    c.finish();

    assertEquals(1, events.size());
    assertEquals(2L, events.get(0).getBytes());
  }

  @Test
  public void finishEmitsOnce() {
    OpContext c = ctx();
    c.finish();
    c.finish();
    assertEquals(1, events.size());
  }

  @Test
  public void estimatedBytesGoToTheFinalEvent() {
    OpContext c = ctx();
    c.addEstimatedBytes(4096);
    c.finish();
    assertEquals(4096L, events.get(0).getEstimatedBytes());
  }

  @Test
  public void pathIsCarried() {
    OpContext c = ctx();
    c.setPath("/user/alice/f");
    c.finish();
    assertEquals("/user/alice/f", events.get(0).getPath());
  }

  @Test
  public void bytesAfterFinishAreIgnored() {
    OpContext c = ctx();
    c.finish();
    clock.advanceSec(20);
    c.addBytes(10);
    assertEquals(1, events.size());
  }
}
