package org.hadooptraffic.context;

import static org.junit.Assert.assertEquals;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import org.apache.hadoop.hdfs.net.Peer;
import org.hadooptraffic.model.Locality;
import org.junit.Test;

public class LocalityResolverTest {
  @Test
  public void loopbackIsLocal() throws Exception {
    assertEquals(Locality.LOCAL,
        LocalityResolver.of(new InetSocketAddress(InetAddress.getLoopbackAddress(), 1)));
    assertEquals(Locality.LOCAL, LocalityResolver.ofHost("127.0.0.1"));
  }

  @Test
  public void documentationAddressIsRemote() throws Exception {
    assertEquals(Locality.REMOTE,
        LocalityResolver.of(new InetSocketAddress(InetAddress.getByName("192.0.2.1"), 1)));
    assertEquals(Locality.REMOTE, LocalityResolver.ofHost("192.0.2.1"));
  }

  @Test
  public void unresolvedOrMissingIsRemote() {
    assertEquals(Locality.REMOTE,
        LocalityResolver.of(InetSocketAddress.createUnresolved("example.invalid", 1)));
    assertEquals(Locality.REMOTE, LocalityResolver.of((java.net.SocketAddress) null));
    assertEquals(Locality.REMOTE, LocalityResolver.of((Peer) null));
    assertEquals(Locality.REMOTE, LocalityResolver.ofHost(null));
  }
}
