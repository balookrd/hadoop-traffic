package org.hadooptraffic.aggregate;

import java.util.concurrent.atomic.LongAdder;

/** Cumulative counters for one key. Thread-safe. */
public final class TrafficCounters {
  private final LongAdder bytes = new LongAdder();
  private final LongAdder estimatedBytes = new LongAdder();
  private final LongAdder ops = new LongAdder();

  void record(long b, long estimated) {
    if (b > 0) {
      bytes.add(b);
    }
    if (estimated > 0) {
      estimatedBytes.add(estimated);
    }
    ops.increment();
  }

  public Snapshot snapshot() {
    return new Snapshot(bytes.sum(), estimatedBytes.sum(), ops.sum());
  }

  /** Point-in-time copy of the counters. */
  public static final class Snapshot {
    private final long bytes;
    private final long estimatedBytes;
    private final long ops;

    public Snapshot(long bytes, long estimatedBytes, long ops) {
      this.bytes = bytes;
      this.estimatedBytes = estimatedBytes;
      this.ops = ops;
    }

    public long getBytes() { return bytes; }
    public long getEstimatedBytes() { return estimatedBytes; }
    public long getOps() { return ops; }
  }
}
