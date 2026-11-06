package org.hadooptraffic.context;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.hadooptraffic.model.Locality;
import org.hadooptraffic.model.Op;
import org.hadooptraffic.model.OpIds;
import org.hadooptraffic.model.Role;
import org.hadooptraffic.model.TrafficEvent;
import org.hadooptraffic.sink.TrafficSinks;

/**
 * Mutable state of one in-flight operation. Emits an intermediate event with the bytes
 * accumulated since the previous emission once per flush interval, and the remainder with
 * {@code last = true} on {@link #finish()}.
 */
public final class OpContext {
  private static volatile Clock clock = Clock.SYSTEM;
  private static volatile long flushIntervalNanos;

  private final String user;
  private final Op op;
  private final Role role;
  private final Locality locality;
  private final String blockPoolId;
  private final long blockId;
  private final String peer;
  private final long opSeq = OpIds.nextSeq();
  private final long startNanos;
  private final long startMillis;
  // Atomics: WebHDFS contexts may be touched from more than one thread.
  private final AtomicLong bytes = new AtomicLong();
  private final AtomicLong estimatedBytes = new AtomicLong();
  private final AtomicBoolean finished = new AtomicBoolean();
  private volatile String path;
  // Written under the lock; read without it as a cheap "not due yet" check.
  private volatile long lastEmitNanos;
  // Guarded by this.
  private long emittedBytes;
  private long emittedEstimatedBytes;

  public OpContext(String user, Op op, Role role, Locality locality, String blockPoolId,
      long blockId, String peer) {
    this.user = user;
    this.op = op;
    this.role = role;
    this.locality = locality;
    this.blockPoolId = blockPoolId;
    this.blockId = blockId;
    this.peer = peer;
    Clock c = clock;
    this.startNanos = c.nanoTime();
    this.startMillis = c.currentTimeMillis();
    this.lastEmitNanos = startNanos;
  }

  /** Interval of intermediate events for all operations; 0 disables them. */
  public static void setFlushIntervalSec(int sec) {
    flushIntervalNanos = TimeUnit.SECONDS.toNanos(Math.max(0, sec));
  }

  /** For tests. */
  static void setClock(Clock c) {
    clock = c;
  }

  /** Printable id of this operation (formats a string: not for the data path). */
  public String opId() {
    return OpIds.format(opSeq);
  }

  public void setPath(String p) {
    path = p;
  }

  public void addBytes(long n) {
    if (n > 0) {
      bytes.addAndGet(n);
      maybeEmitDelta();
    }
  }

  public void addEstimatedBytes(long n) {
    if (n > 0) {
      estimatedBytes.addAndGet(n);
    }
  }

  public long bytes() {
    return bytes.get();
  }

  private void maybeEmitDelta() {
    long interval = flushIntervalNanos;
    if (interval <= 0) {
      return;
    }
    long now = clock.nanoTime();
    if (now - lastEmitNanos < interval) {
      return;
    }
    synchronized (this) {
      if (finished.get() || now - lastEmitNanos < interval) {
        return;
      }
      emit(now, false);
    }
  }

  /** Emits the remainder as the final event; only the first call has an effect. */
  public void finish() {
    if (!finished.compareAndSet(false, true)) {
      return;
    }
    synchronized (this) {
      emit(clock.nanoTime(), true);
    }
  }

  /** Caller holds the lock. Emitting under the lock keeps one operation's events ordered. */
  private void emit(long nowNanos, boolean last) {
    long b = bytes.get();
    long est = estimatedBytes.get();
    long deltaBytes = b - emittedBytes;
    long deltaEstimated = est - emittedEstimatedBytes;
    lastEmitNanos = nowNanos;
    if (!last && deltaBytes == 0 && deltaEstimated == 0) {
      return;
    }
    emittedBytes = b;
    emittedEstimatedBytes = est;
    TrafficSinks.emit(new TrafficEvent(user, op, role, locality, blockPoolId, blockId,
        deltaBytes, deltaEstimated, (nowNanos - startNanos) / 1_000_000L, peer, opSeq,
        startMillis, clock.currentTimeMillis(), last, path));
  }
}
