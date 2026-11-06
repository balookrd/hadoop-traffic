package org.hadooptraffic.hooks;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.security.UserGroupInformation;
import org.hadooptraffic.context.OpContexts;
import org.hadooptraffic.it.TestTokens;
import org.hadooptraffic.model.Locality;
import org.hadooptraffic.model.Op;
import org.hadooptraffic.model.Role;
import org.hadooptraffic.model.TrafficEvent;
import org.hadooptraffic.sink.TrafficSinks;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class XceiverHooksTest {
  private final List<TrafficEvent> events = new ArrayList<>();

  @Before
  public void setUp() {
    TrafficSinks.clear();
    TrafficSinks.register(events::add);
  }

  @After
  public void tearDown() {
    OpContexts.end(true);
    TrafficSinks.clear();
  }

  private static ExtendedBlock block(long len) {
    return new ExtendedBlock("bp-1", 42L, len, 1L);
  }

  @Test
  public void readBlockAccumulatesPacketsAndEmitsOnce() {
    Object[] args = {block(100), TestTokens.blockToken("alice", false),
        "DFSClient_NONMAPREDUCE_1_1", 0L, 100L, true, null};

    boolean started = XceiverHooks.onXceiverEnter("readBlock", args, null);
    OpContexts.addBytes(60);
    OpContexts.addBytes(40);
    OpContexts.addBytes(-1);
    OpContexts.end(started);
    OpContexts.end(started);

    assertTrue(started);
    assertNull(OpContexts.current());
    assertEquals(1, events.size());
    TrafficEvent e = events.get(0);
    assertEquals("alice", e.getUser());
    assertEquals(Op.READ, e.getOp());
    assertEquals(Role.CLIENT, e.getRole());
    assertEquals(Locality.REMOTE, e.getLocality());
    assertEquals("bp-1", e.getBlockPoolId());
    assertEquals(42L, e.getBlockId());
    assertEquals(100L, e.getBytes());
  }

  @Test
  public void staleContextIsFinishedAndReplacedByNextXceiverOp() {
    Object[] staleArgs = {block(100), TestTokens.blockToken("stale", false),
        "DFSClient_NONMAPREDUCE_1_1", 0L, 100L, true, null};
    Object[] freshArgs = {block(100), TestTokens.blockToken("fresh", false),
        "DFSClient_NONMAPREDUCE_1_1", 0L, 100L, true, null};

    // Simulates a missed end on this pooled DataXceiver thread.
    XceiverHooks.onXceiverEnter("readBlock", staleArgs, null);
    OpContexts.addBytes(5);

    boolean started = XceiverHooks.onXceiverEnter("readBlock", freshArgs, null);
    OpContexts.addBytes(7);
    OpContexts.end(started);

    assertTrue(started);
    assertEquals(2, events.size());
    assertEquals("stale", events.get(0).getUser());
    assertEquals(5L, events.get(0).getBytes());
    assertEquals("fresh", events.get(1).getUser());
    assertEquals(7L, events.get(1).getBytes());
    assertNull(OpContexts.current());
  }

  @Test
  public void writeBlockUsesPipelinePosition() {
    Object[] args = {block(0), null, TestTokens.blockToken("bob", true),
        "DFSClient_NONMAPREDUCE_1_1", new DatanodeInfo[1], null, null,
        BlockConstructionStage.PIPELINE_SETUP_CREATE, 3, 0L, 0L, 0L};

    boolean started = XceiverHooks.onXceiverEnter("writeBlock", args, null);
    OpContexts.end(started);

    assertEquals(Role.PIPELINE, events.get(0).getRole());
    assertEquals("bob", events.get(0).getUser());
  }

  @Test
  public void shortCircuitRecordsEstimatedBytes() {
    Object[] args = {block(4096), TestTokens.blockToken("carol", false), null, 1, true};

    boolean started = XceiverHooks.onXceiverEnter("requestShortCircuitFds", args, null);
    OpContexts.end(started);

    TrafficEvent e = events.get(0);
    assertEquals(Op.SCR_FDS, e.getOp());
    assertEquals(0L, e.getBytes());
    assertEquals(4096L, e.getEstimatedBytes());
  }

  @Test
  public void dataTransferNestedInXceiverOpDoesNotStartNewContext() {
    Object[] args = {block(10), TestTokens.blockToken("dave", false), "DFSClient_x",
        new DatanodeInfo[0], null, null};
    boolean outer = XceiverHooks.onXceiverEnter("transferBlock", args, null);

    boolean inner = XceiverHooks.onDataTransferEnter(block(10), null);
    OpContexts.addBytes(10);
    OpContexts.end(inner);
    OpContexts.end(outer);

    assertFalse(inner);
    assertEquals(1, events.size());
    assertEquals(Op.TRANSFER, events.get(0).getOp());
    assertEquals(10L, events.get(0).getBytes());
  }

  @Test
  public void standaloneDataTransferIsAttributedToDatanodeUser() throws Exception {
    boolean started = XceiverHooks.onDataTransferEnter(block(10), null);
    OpContexts.addBytes(10);
    OpContexts.end(started);

    TrafficEvent e = events.get(0);
    assertEquals(UserGroupInformation.getCurrentUser().getShortUserName(), e.getUser());
    assertEquals(Op.TRANSFER, e.getOp());
    assertEquals(Role.DATANODE, e.getRole());
  }

  @Test
  public void unknownMethodIsIgnored() {
    assertFalse(XceiverHooks.onXceiverEnter("blockChecksum", new Object[0], null));
    assertTrue(events.isEmpty());
  }
}
