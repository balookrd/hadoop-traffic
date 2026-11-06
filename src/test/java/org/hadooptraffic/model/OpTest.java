package org.hadooptraffic.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class OpTest {
  @Test
  public void mapsXceiverMethods() {
    assertEquals(Op.READ, Op.fromXceiverMethod("readBlock"));
    assertEquals(Op.WRITE, Op.fromXceiverMethod("writeBlock"));
    assertEquals(Op.TRANSFER, Op.fromXceiverMethod("transferBlock"));
    assertEquals(Op.COPY, Op.fromXceiverMethod("copyBlock"));
    assertEquals(Op.REPLACE, Op.fromXceiverMethod("replaceBlock"));
    assertEquals(Op.SCR_FDS, Op.fromXceiverMethod("requestShortCircuitFds"));
    assertNull(Op.fromXceiverMethod("blockChecksum"));
    assertNull(Op.fromXceiverMethod(null));
  }

  @Test
  public void mapsWebHdfsMethods() {
    assertEquals(Op.WEBHDFS_READ, Op.fromWebHdfsMethod("onOpen"));
    assertEquals(Op.WEBHDFS_CREATE, Op.fromWebHdfsMethod("onCreate"));
    assertEquals(Op.WEBHDFS_APPEND, Op.fromWebHdfsMethod("onAppend"));
    assertNull(Op.fromWebHdfsMethod("onGetFileChecksum"));
  }

  @Test
  public void labelsAreStable() {
    assertEquals("scr_fds", Op.SCR_FDS.label());
    assertEquals("webhdfs_create", Op.WEBHDFS_CREATE.label());
    assertEquals("webhdfs_proxy", Role.WEBHDFS_PROXY.label());
    assertEquals("remote", Locality.REMOTE.label());
  }
}
