package org.hadooptraffic.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TrafficEventTest {
  @Test
  public void fileBlockIdKeepsContiguousIds() {
    assertEquals(1073741901L, TrafficEvent.fileBlockId(1073741901L));
  }

  @Test
  public void fileBlockIdMasksStripedIndex() {
    long group = Long.MIN_VALUE + 16L * 5;
    assertEquals(group, TrafficEvent.fileBlockId(group + 3));
    assertEquals(group, TrafficEvent.fileBlockId(group));
  }

  @Test
  public void fileBlockIdKeepsNotApplicableMarker() {
    assertEquals(-1L, TrafficEvent.fileBlockId(-1L));
  }

  @Test
  public void fullConstructorKeepsAllFields() {
    TrafficEvent e = new TrafficEvent("alice", Op.READ, Role.CLIENT, Locality.REMOTE, "bp",
        Long.MIN_VALUE + 35, 10L, 2L, 300L, "peer", 1L, 1000L, 1300L, false, "/a");
    assertEquals(OpIds.format(1L), e.getOpId());
    assertEquals(1000L, e.getStartMillis());
    assertEquals(1300L, e.getTimestampMillis());
    assertEquals(false, e.isLast());
    assertEquals("/a", e.getPath());
    assertEquals(Long.MIN_VALUE + 32, e.getFileBlockId());
  }

  @Test
  public void legacyConstructorDescribesCompleteOperation() {
    long before = System.currentTimeMillis();
    TrafficEvent e = new TrafficEvent("alice", Op.READ, Role.CLIENT, Locality.LOCAL, "bp",
        7L, 10L, 0L, 5L, "p");
    assertTrue(e.isLast());
    assertNull(e.getOpId());
    assertNull(e.getPath());
    assertTrue(e.getTimestampMillis() >= before);
    assertEquals(e.getTimestampMillis() - 5L, e.getStartMillis());
    assertEquals(7L, e.getFileBlockId());
  }
}
