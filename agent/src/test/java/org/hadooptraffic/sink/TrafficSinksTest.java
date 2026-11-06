package org.hadooptraffic.sink;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.hadooptraffic.model.Locality;
import org.hadooptraffic.model.Op;
import org.hadooptraffic.model.Role;
import org.hadooptraffic.model.TrafficEvent;
import org.junit.After;
import org.junit.Test;

public class TrafficSinksTest {
  @After
  public void tearDown() {
    TrafficSinks.clear();
  }

  @Test
  public void emitReachesAllSinksEvenIfOneFails() {
    List<TrafficEvent> got = new ArrayList<>();
    TrafficSinks.register(e -> {
      throw new RuntimeException("boom");
    });
    TrafficSinks.register(got::add);
    TrafficEvent e = sample();

    TrafficSinks.emit(e);

    assertEquals(Collections.singletonList(e), got);
  }

  @Test
  public void emitReachesAllSinksEvenIfOneThrowsAnError() {
    List<TrafficEvent> got = new ArrayList<>();
    TrafficSinks.register(e -> {
      throw new AssertionError("boom");
    });
    TrafficSinks.register(got::add);
    TrafficEvent e = sample();

    TrafficSinks.emit(e);

    assertEquals(Collections.singletonList(e), got);
  }

  @Test
  public void emitWithoutSinksIsNoop() {
    TrafficSinks.emit(sample());
  }

  static TrafficEvent sample() {
    return new TrafficEvent("alice", Op.READ, Role.CLIENT, Locality.LOCAL,
        "bp-1", 1L, 10L, 0L, 5L, "127.0.0.1:1");
  }
}
