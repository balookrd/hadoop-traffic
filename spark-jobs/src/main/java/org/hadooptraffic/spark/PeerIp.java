package org.hadooptraffic.spark;

/**
 * Address of the other side from the agent's {@code peer} field, without the port. The field
 * holds one of: {@code /ip:port} or {@code host/ip:port} (TCP peers, WebHDFS channels),
 * {@code ip:port} (DataNode-initiated transfers), {@code unix:<path>} (domain sockets).
 * Returns null when there is no network address.
 */
public final class PeerIp {
  private static final String UNRESOLVED = "/<unresolved>";

  private PeerIp() {
  }

  public static String of(String peer) {
    if (peer == null || peer.isEmpty() || peer.startsWith("unix:")) {
      return null;
    }
    String s = peer;
    int unresolved = s.indexOf(UNRESOLVED);
    if (unresolved > 0) {
      // Java 14+ form of an unresolved address: "host/<unresolved>:port".
      s = s.substring(0, unresolved) + s.substring(unresolved + UNRESOLVED.length());
    } else {
      int slash = s.lastIndexOf('/');
      if (slash >= 0) {
        s = s.substring(slash + 1);
      }
    }
    if (s.startsWith("[")) {
      int end = s.indexOf(']');
      return end > 1 ? s.substring(1, end) : null;
    }
    int colon = s.lastIndexOf(':');
    if (colon >= 0) {
      s = s.substring(0, colon);
    }
    return s.isEmpty() ? null : s;
  }
}
