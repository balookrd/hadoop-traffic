package org.hadooptraffic.context;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.hadooptraffic.model.Locality;
import org.hadooptraffic.model.Op;
import org.hadooptraffic.model.Role;
import org.hadooptraffic.model.TrafficEvent;
import org.hadooptraffic.sink.TrafficSinks;

/** Mutable state of one in-flight operation. */
public final class OpContext {
  private final String user;
  private final Op op;
  private final Role role;
  private final Locality locality;
  private final String blockPoolId;
  private final long blockId;
  private final String peer;
  private final long startNanos = System.nanoTime();
  // Atomics: WebHDFS contexts may be touched from more than one thread.
  private final AtomicLong bytes = new AtomicLong();
  private final AtomicLong estimatedBytes = new AtomicLong();
  private final AtomicBoolean finished = new AtomicBoolean();

  public OpContext(String user, Op op, Role role, Locality locality, String blockPoolId,
      long blockId, String peer) {
    this.user = user;
    this.op = op;
    this.role = role;
    this.locality = locality;
    this.blockPoolId = blockPoolId;
    this.blockId = blockId;
    this.peer = peer;
  }

  public void addBytes(long n) {
    if (n > 0) {
      bytes.addAndGet(n);
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

  /** Emits the event to all sinks; only the first call has an effect. */
  public void finish() {
    if (!finished.compareAndSet(false, true)) {
      return;
    }
    long durationMs = (System.nanoTime() - startNanos) / 1_000_000L;
    TrafficSinks.emit(new TrafficEvent(user, op, role, locality, blockPoolId, blockId,
        bytes.get(), estimatedBytes.get(), durationMs, peer));
  }
}
