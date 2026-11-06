package org.hadooptraffic.context;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Addresses of this host's network interfaces, re-read at most once per refresh interval.
 * Replaces a per-call NetworkInterface.getByInetAddress (which enumerates every interface
 * with syscalls) on the data path: a lookup is a set probe, and a due refresh is done by one
 * caller while the others keep using the previous set.
 */
final class LocalAddresses {
  static final long DEFAULT_REFRESH_SEC = 60;

  /** Reads the current interface addresses; may throw. */
  interface Source {
    Set<InetAddress> read() throws Exception;
  }

  private static final class Snapshot {
    final Set<InetAddress> addresses;
    final long readAtNanos;

    Snapshot(Set<InetAddress> addresses, long readAtNanos) {
      this.addresses = addresses;
      this.readAtNanos = readAtNanos;
    }
  }

  private final Source source;
  private final Clock clock;
  private final long refreshNanos;
  private final AtomicBoolean refreshing = new AtomicBoolean();
  private volatile Snapshot snapshot;

  LocalAddresses(Source source, Clock clock, long refreshSec) {
    this.source = source;
    this.clock = clock;
    this.refreshNanos = TimeUnit.SECONDS.toNanos(refreshSec);
  }

  static LocalAddresses system() {
    return new LocalAddresses(LocalAddresses::readInterfaces, Clock.SYSTEM, DEFAULT_REFRESH_SEC);
  }

  /** True if a is assigned to one of this host's interfaces (as of the last refresh). */
  boolean contains(InetAddress a) {
    Snapshot s = snapshot;
    if (s == null || clock.nanoTime() - s.readAtNanos >= refreshNanos) {
      s = refresh(s);
    }
    return s.addresses.contains(a);
  }

  private Snapshot refresh(Snapshot old) {
    if (old == null) {
      synchronized (this) {
        if (snapshot == null) {
          snapshot = load(Collections.<InetAddress>emptySet());
        }
        return snapshot;
      }
    }
    if (!refreshing.compareAndSet(false, true)) {
      return old; // another thread is re-reading: a slightly stale set is fine
    }
    try {
      Snapshot s = load(old.addresses);
      snapshot = s;
      return s;
    } finally {
      refreshing.set(false);
    }
  }

  /** On failure keeps the previous addresses and retries after the next interval. */
  private Snapshot load(Set<InetAddress> fallback) {
    Set<InetAddress> addresses;
    try {
      addresses = source.read();
      if (addresses == null) {
        addresses = fallback;
      }
    } catch (Throwable t) {
      addresses = fallback;
    }
    return new Snapshot(addresses, clock.nanoTime());
  }

  private static Set<InetAddress> readInterfaces() throws Exception {
    Set<InetAddress> out = new HashSet<>();
    for (NetworkInterface ni : Collections.list(NetworkInterface.getNetworkInterfaces())) {
      add(ni, out);
    }
    return Collections.unmodifiableSet(out);
  }

  private static void add(NetworkInterface ni, Set<InetAddress> out) {
    out.addAll(Collections.list(ni.getInetAddresses()));
    for (NetworkInterface sub : Collections.list(ni.getSubInterfaces())) {
      add(sub, out);
    }
  }
}
