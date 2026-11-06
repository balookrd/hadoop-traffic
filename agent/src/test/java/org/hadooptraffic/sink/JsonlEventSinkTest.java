package org.hadooptraffic.sink;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.hadooptraffic.model.Locality;
import org.hadooptraffic.model.Op;
import org.hadooptraffic.model.Role;
import org.hadooptraffic.model.TrafficEvent;
import org.hadooptraffic.testutil.FakeClock;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class JsonlEventSinkTest {
  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  private final FakeClock clock = new FakeClock(1_793_960_100_000L);
  private JsonlEventSink sink;

  @After
  public void tearDown() {
    if (sink != null) {
      sink.close(2000);
    }
  }

  private JsonlEventSink sink(Path active, int capacity) {
    sink = new JsonlEventSink(active, capacity, 1 << 20, 300_000, 10, () -> "dn1", clock);
    return sink;
  }

  private static TrafficEvent ev(long bytes) {
    return new TrafficEvent("alice", Op.READ, Role.CLIENT, Locality.LOCAL, "bp", 1L, bytes, 0L,
        1L, "p");
  }

  @Test
  public void writesAcceptedEvents() throws Exception {
    Path active = tmp.getRoot().toPath().resolve("events.jsonl");
    JsonlEventSink s = sink(active, 100);
    s.start();
    s.onEvent(ev(1));
    s.onEvent(ev(2));
    s.onEvent(ev(3));
    assertTrue(s.awaitIdle(5000));

    List<String> lines = Files.readAllLines(active, UTF_8);
    assertEquals(3, lines.size());
    assertTrue(lines.get(0).startsWith("{\"ts\":"));
    assertTrue(lines.get(0).contains("\"dn\":\"dn1\""));
    assertEquals(3L, s.eventsWritten());
    assertEquals(0L, s.eventsDropped());
  }

  @Test
  public void dropsWhenQueueIsFull() throws Exception {
    Path active = tmp.getRoot().toPath().resolve("events.jsonl");
    JsonlEventSink s = sink(active, 2);
    s.onEvent(ev(1));
    s.onEvent(ev(2));
    s.onEvent(ev(3));          // writer not started: queue is full
    assertEquals(1L, s.eventsDropped());
    assertEquals(2L, s.queueSize());

    s.start();
    assertTrue(s.awaitIdle(5000));
    assertEquals(2L, s.eventsWritten());
    assertEquals(0L, s.queueSize());
  }

  @Test
  public void unwritableLocationDropsThenRecovers() throws Exception {
    Path blocker = tmp.newFile("blocker").toPath();
    Path active = blocker.resolve("events.jsonl"); // parent is a regular file
    JsonlEventSink s = sink(active, 100);
    s.start();
    s.onEvent(ev(1));
    assertTrue(s.awaitIdle(5000));
    assertEquals(1L, s.eventsDropped());
    assertEquals(0L, s.eventsWritten());

    Files.delete(blocker);
    s.onEvent(ev(2));
    assertTrue(s.awaitIdle(5000));
    assertEquals(1L, s.eventsWritten());
    assertTrue(Files.exists(active));
  }

  @Test
  public void closeWritesQueuedEventsAndRotates() throws Exception {
    Path active = tmp.getRoot().toPath().resolve("events.jsonl");
    JsonlEventSink s = sink(active, 100);
    s.start();
    s.onEvent(ev(1));
    s.close(2000);
    sink = null;

    assertFalse(Files.exists(active));
    try (java.util.stream.Stream<Path> files = Files.list(tmp.getRoot().toPath())) {
      Path closed = files.filter(p -> p.getFileName().toString().startsWith("events-"))
          .findFirst().get();
      assertEquals(1, Files.readAllLines(closed, UTF_8).size());
    }
  }

  @Test
  public void malformedEventIsDroppedAndNotWritten() throws Exception {
    Path active = tmp.getRoot().toPath().resolve("events.jsonl");
    JsonlEventSink s = sink(active, 100);
    s.start();
    // Null op: EventJson.format throws NullPointerException.
    s.onEvent(new TrafficEvent("alice", null, Role.CLIENT, Locality.LOCAL, "bp", 1L, 1L, 0L,
        1L, "p"));
    assertTrue(s.awaitIdle(5000));
    assertEquals(1L, s.eventsDropped());
    assertEquals(0L, s.eventsWritten());

    s.onEvent(ev(2));
    assertTrue(s.awaitIdle(5000));
    assertEquals(1L, s.eventsWritten());
    assertEquals(1, Files.readAllLines(active, UTF_8).size());
  }

  @Test
  public void eventsAfterCloseAreCountedAsDropped() throws Exception {
    Path active = tmp.getRoot().toPath().resolve("events.jsonl");
    JsonlEventSink s = sink(active, 100);
    s.start();
    s.close(2000);
    sink = null;
    s.onEvent(ev(1));
    assertEquals(1L, s.eventsDropped());
    assertEquals(0L, s.queueSize());
  }

  @Test
  public void closeOfNeverStartedSinkCountsQueuedEventsAsDropped() throws Exception {
    Path active = tmp.getRoot().toPath().resolve("events.jsonl");
    JsonlEventSink s = sink(active, 100);
    s.onEvent(ev(1));
    s.onEvent(ev(2));
    s.close(2000);
    sink = null;
    assertEquals(2L, s.eventsDropped());
    assertTrue(s.awaitIdle(100));
  }

  @Test
  public void closeReturnsQuicklyOnAnIdleSink() throws Exception {
    Path active = tmp.getRoot().toPath().resolve("events.jsonl");
    JsonlEventSink s = sink(active, 100);
    s.start();
    long t0 = System.nanoTime();
    s.close(2000);
    sink = null;
    long ms = (System.nanoTime() - t0) / 1_000_000L;
    assertTrue("close took " + ms + " ms", ms < 1000);
  }

  @Test
  public void failingHostNameLookupFallsBackToUnknown() throws Exception {
    Path active = tmp.getRoot().toPath().resolve("events.jsonl");
    sink = new JsonlEventSink(active, 100, 1 << 20, 300_000, 10,
        () -> { throw new IllegalStateException("no DNS"); }, clock);
    sink.start();
    sink.onEvent(ev(1));
    assertTrue(sink.awaitIdle(5000));
    assertTrue(Files.readAllLines(active, UTF_8).get(0).contains("\"dn\":\"unknown\""));
  }

  @Test
  public void malformedEventDoesNotDropTheBatch() throws Exception {
    Path active = tmp.getRoot().toPath().resolve("events.jsonl");
    JsonlEventSink s = sink(active, 100);
    s.onEvent(ev(1));
    s.onEvent(new TrafficEvent("alice", null, Role.CLIENT, Locality.LOCAL, "bp", 1L, 1L, 0L,
        1L, "p"));
    s.onEvent(ev(3));
    s.start();
    assertTrue(s.awaitIdle(5000));
    assertEquals(2L, s.eventsWritten());
    assertEquals(1L, s.eventsDropped());
    assertEquals(2, Files.readAllLines(active, UTF_8).size());
  }

  @Test
  public void eventsOnEitherSideOfMidnightGoToTwoFiles() throws Exception {
    Path active = tmp.getRoot().toPath().resolve("events.jsonl");
    sink = new JsonlEventSink(active, 100, 1 << 20, 300_000, 10, () -> "dn1", clock);
    JsonlEventSink s = sink;
    long before = 1_793_923_199_000L; // 2026-11-05T23:59:59Z
    s.onEvent(atTs(before));
    s.onEvent(atTs(before + 2_000));
    s.start();
    assertTrue(s.awaitIdle(5000));
    assertEquals(1, Files.readAllLines(active, UTF_8).size());
    assertTrue(Files.exists(tmp.getRoot().toPath().resolve("events-20261105-235959-000000.jsonl")));
  }

  @Test
  public void crashLeftoverIsRotatedAtStartup() throws Exception {
    Path active = tmp.getRoot().toPath().resolve("events.jsonl");
    Files.write(active, "old\n".getBytes(UTF_8));
    JsonlEventSink s = sink(active, 100);
    s.start();
    long deadline = System.currentTimeMillis() + 5000;
    while (System.currentTimeMillis() < deadline && Files.exists(active)) {
      Thread.sleep(20);
    }
    assertFalse(Files.exists(active));
    try (java.util.stream.Stream<Path> files = Files.list(tmp.getRoot().toPath())) {
      assertEquals(1L, files.filter(p -> p.getFileName().toString().startsWith("events-"))
          .count());
    }
  }

  private static TrafficEvent atTs(long ts) {
    return new TrafficEvent("alice", Op.READ, Role.CLIENT, Locality.LOCAL, "bp", 1L, 1L, 0L,
        1L, "p", 1L, ts - 1, ts, true, null);
  }
}
