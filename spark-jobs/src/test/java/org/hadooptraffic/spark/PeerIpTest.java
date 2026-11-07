package org.hadooptraffic.spark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class PeerIpTest {
  @Test
  public void socketAddressStrings() {
    // Peer.getRemoteAddressString() and Netty remoteAddress().toString(): "/ip:port".
    assertEquals("10.0.0.11", PeerIp.of("/10.0.0.11:51234"));
    // Same with a reverse-resolved host name in front.
    assertEquals("10.0.0.11", PeerIp.of("dn1.example.com/10.0.0.11:51234"));
  }

  @Test
  public void xferAddressWithoutSlash() {
    // DatanodeInfo.getXferAddr() of DataNode-initiated transfers: "ip:port".
    assertEquals("10.0.0.13", PeerIp.of("10.0.0.13:50010"));
  }

  @Test
  public void ipv6() {
    // Java 8 prints IPv6 socket addresses without brackets, later versions with them.
    assertEquals("0:0:0:0:0:0:0:1", PeerIp.of("/0:0:0:0:0:0:0:1:50010"));
    assertEquals("2001:db8:0:0:0:0:0:1", PeerIp.of("/[2001:db8:0:0:0:0:0:1]:50010"));
  }

  @Test
  public void unresolvedHost() {
    assertEquals("dn9.example.com", PeerIp.of("dn9.example.com:50010"));
    assertEquals("dn9.example.com", PeerIp.of("dn9.example.com/<unresolved>:50010"));
  }

  @Test
  public void noAddress() {
    assertNull(PeerIp.of(null));
    assertNull(PeerIp.of(""));
    assertNull(PeerIp.of("unix:/var/lib/hadoop-hdfs/dn_socket"));
    assertNull(PeerIp.of("/:50010"));
    assertNull(PeerIp.of("/[]:50010"));
  }
}
