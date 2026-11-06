package org.hadooptraffic.context;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.net.NetUtils;
import org.hadooptraffic.model.Locality;

/** Decides whether a peer runs on this host. */
public final class LocalityResolver {
  private LocalityResolver() {
  }

  public static Locality of(Peer peer) {
    if (peer == null) {
      return Locality.REMOTE;
    }
    try {
      return peer.getDomainSocket() != null || peer.isLocal() ? Locality.LOCAL : Locality.REMOTE;
    } catch (RuntimeException e) {
      return Locality.REMOTE;
    }
  }

  public static Locality of(SocketAddress address) {
    if (address instanceof InetSocketAddress) {
      return of(((InetSocketAddress) address).getAddress());
    }
    return Locality.REMOTE;
  }

  /** Locality of a literal IP address (as in DatanodeInfo.getIpAddr()). */
  public static Locality ofHost(String ip) {
    if (ip == null || ip.isEmpty()) {
      return Locality.REMOTE;
    }
    try {
      return of(InetAddress.getByName(ip));
    } catch (Exception e) {
      return Locality.REMOTE;
    }
  }

  private static Locality of(InetAddress a) {
    if (a == null) {
      return Locality.REMOTE;
    }
    // Checks the address against the local network interfaces (no per-address cache).
    return a.isLoopbackAddress() || NetUtils.isLocalAddress(a) ? Locality.LOCAL : Locality.REMOTE;
  }
}
