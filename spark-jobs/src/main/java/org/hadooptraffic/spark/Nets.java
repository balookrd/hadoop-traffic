package org.hadooptraffic.spark;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Named networks for the {@code origin} column of traffic_minute, from
 * {@code --nets name=cidr[,name=cidr...]}, e.g. {@code own=10.0.0.0/24,cluster_b=172.16.0.0/24}.
 * A name may repeat to cover several CIDRs. The first matching CIDR wins. Addresses are matched
 * only as IP literals; a host name never triggers a DNS lookup and gets {@link #OTHER}.
 */
public final class Nets implements Serializable {
  private static final long serialVersionUID = 1L;

  /** Origin of events with locality = local: the client is on the DataNode's own host. */
  public static final String LOCAL = "local";
  /** Origin of a remote peer outside every configured network, or without an IP literal. */
  public static final String OTHER = "other";

  public static final Nets NONE = new Nets(Collections.<Net>emptyList());

  private static final Pattern NAME = Pattern.compile("[a-z][a-z0-9_]*");
  private static final Pattern IPV4 =
      Pattern.compile("(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})");

  private static final class Net implements Serializable {
    private static final long serialVersionUID = 1L;
    final String name;
    final byte[] prefix;
    final int bits;

    Net(String name, byte[] prefix, int bits) {
      this.name = name;
      this.prefix = prefix;
      this.bits = bits;
    }

    boolean contains(byte[] a) {
      if (a.length != prefix.length) {
        return false;
      }
      int full = bits / 8;
      for (int i = 0; i < full; i++) {
        if (a[i] != prefix[i]) {
          return false;
        }
      }
      int rest = bits % 8;
      if (rest == 0) {
        return true;
      }
      int mask = (0xff << (8 - rest)) & 0xff;
      return (a[full] & mask) == (prefix[full] & mask);
    }
  }

  private final List<Net> nets;

  private Nets(List<Net> nets) {
    this.nets = nets;
  }

  public boolean isEmpty() {
    return nets.isEmpty();
  }

  /** Parses {@code name=cidr[,name=cidr...]}; throws IllegalArgumentException on bad input. */
  public static Nets parse(String spec) {
    List<Net> out = new ArrayList<>();
    for (String item : spec.split(",", -1)) {
      int eq = item.indexOf('=');
      if (eq < 0) {
        throw new IllegalArgumentException("expected name=cidr: " + item);
      }
      String name = item.substring(0, eq);
      if (!NAME.matcher(name).matches() || LOCAL.equals(name) || OTHER.equals(name)) {
        throw new IllegalArgumentException("bad network name (lower-case [a-z0-9_], not '"
            + LOCAL + "' or '" + OTHER + "'): " + name);
      }
      String cidr = item.substring(eq + 1);
      int slash = cidr.indexOf('/');
      if (slash < 0) {
        throw new IllegalArgumentException("expected address/length: " + cidr);
      }
      byte[] addr = literal(cidr.substring(0, slash));
      if (addr == null) {
        throw new IllegalArgumentException("not an IP address: " + cidr);
      }
      int bits;
      try {
        bits = Integer.parseInt(cidr.substring(slash + 1));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("bad prefix length: " + cidr);
      }
      if (bits < 0 || bits > addr.length * 8) {
        throw new IllegalArgumentException("bad prefix length: " + cidr);
      }
      Net net = new Net(name, addr, bits);
      if (!hostBitsZero(addr, bits)) {
        throw new IllegalArgumentException("host bits set (typo?): " + cidr);
      }
      out.add(net);
    }
    return new Nets(Collections.unmodifiableList(out));
  }

  /** Name of the first network containing ip, or {@link #OTHER}. */
  public String originOf(String ip) {
    if (ip == null || nets.isEmpty()) {
      return OTHER;
    }
    byte[] a = literal(ip);
    if (a == null) {
      return OTHER;
    }
    for (Net n : nets) {
      if (n.contains(a)) {
        return n.name;
      }
    }
    return OTHER;
  }

  private static boolean hostBitsZero(byte[] a, int bits) {
    for (int i = 0; i < a.length * 8; i++) {
      if (i >= bits && (a[i / 8] & (0x80 >>> (i % 8))) != 0) {
        return false;
      }
    }
    return true;
  }

  /**
   * Bytes of an IPv4 dotted quad or an IPv6 literal (an IPv4-mapped IPv6 address becomes 4
   * bytes); null for anything else. Never resolves names.
   */
  static byte[] literal(String s) {
    java.util.regex.Matcher m = IPV4.matcher(s);
    if (m.matches()) {
      byte[] b = new byte[4];
      for (int i = 0; i < 4; i++) {
        int v = Integer.parseInt(m.group(i + 1));
        if (v > 255) {
          return null;
        }
        b[i] = (byte) v;
      }
      return b;
    }
    // Only strings with ':' are passed to InetAddress: it parses them as IPv6 literals and
    // does not query DNS.
    if (s.indexOf(':') < 0) {
      return null;
    }
    try {
      return InetAddress.getByName(s).getAddress();
    } catch (UnknownHostException | SecurityException e) {
      return null;
    }
  }
}
