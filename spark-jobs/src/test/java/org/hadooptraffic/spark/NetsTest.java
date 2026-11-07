package org.hadooptraffic.spark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import org.junit.Test;

public class NetsTest {
  @Test
  public void matchesFirstNetwork() {
    Nets n = Nets.parse("own=10.0.0.0/24,cluster_b=172.16.0.0/24,own=10.0.4.0/22");
    assertEquals("own", n.originOf("10.0.0.7"));
    assertEquals("cluster_b", n.originOf("172.16.0.255"));
    assertEquals("own", n.originOf("10.0.7.1"));
    assertEquals(Nets.OTHER, n.originOf("10.0.8.1"));
    assertEquals(Nets.OTHER, n.originOf("172.16.1.1"));
    assertEquals(Nets.OTHER, n.originOf(null));
  }

  @Test
  public void firstMatchWinsOnOverlap() {
    Nets n = Nets.parse("edge=10.0.0.0/28,own=10.0.0.0/24");
    assertEquals("edge", n.originOf("10.0.0.5"));
    assertEquals("own", n.originOf("10.0.0.16"));
  }

  @Test
  public void prefixLengthsOffByteBoundary() {
    Nets n = Nets.parse("a=10.0.0.0/22,all=0.0.0.0/0");
    assertEquals("a", n.originOf("10.0.3.255"));
    assertEquals("all", n.originOf("10.0.4.0"));
  }

  @Test
  public void ipv6AndMappedAddresses() {
    Nets n = Nets.parse("v6=fd00:1::/64,own=10.0.0.0/24");
    assertEquals("v6", n.originOf("fd00:1::abcd"));
    assertEquals(Nets.OTHER, n.originOf("fd00:2::1"));
    assertEquals("own", n.originOf("::ffff:10.0.0.9"));
  }

  @Test
  public void namesAreNotResolved() {
    Nets n = Nets.parse("all=0.0.0.0/0");
    assertEquals(Nets.OTHER, n.originOf("localhost"));
    assertEquals(Nets.OTHER, n.originOf("dn1.example.com"));
    assertEquals(Nets.OTHER, n.originOf("10.0.0.256"));
    assertNull(Nets.literal("example.com"));
  }

  @Test
  public void noNetworks() {
    assertEquals(Nets.OTHER, Nets.NONE.originOf("10.0.0.1"));
  }

  private static void assertRejected(String spec) {
    try {
      Nets.parse(spec);
    } catch (IllegalArgumentException e) {
      return;
    }
    fail("accepted: " + spec);
  }

  @Test
  public void rejectsBadSpecs() {
    assertRejected("");
    assertRejected("own");
    assertRejected("own=10.0.0.0");
    assertRejected("own=10.0.0.0/33");
    assertRejected("own=10.0.0.0/-1");
    assertRejected("own=10.0.0.0/x");
    assertRejected("own=10.0.0.5/24");          // host bits set
    assertRejected("own=host.example/24");
    assertRejected("Own=10.0.0.0/24");
    assertRejected("local=10.0.0.0/24");
    assertRejected("other=10.0.0.0/24");
    assertRejected("own=10.0.0.0/24,");
    assertRejected("own=10.0.0.0/24;b=1.0.0.0/8");
  }
}
