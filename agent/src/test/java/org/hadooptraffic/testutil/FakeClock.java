package org.hadooptraffic.testutil;

import org.hadooptraffic.context.Clock;

/** Manually advanced clock for deterministic tests. */
public final class FakeClock implements Clock {
  private long nanos;
  private long millis;

  public FakeClock(long startMillis) {
    this.millis = startMillis;
  }

  public synchronized void advanceMillis(long ms) {
    nanos += ms * 1_000_000L;
    millis += ms;
  }

  public void advanceSec(long sec) {
    advanceMillis(sec * 1000L);
  }

  @Override
  public synchronized long nanoTime() {
    return nanos;
  }

  @Override
  public synchronized long currentTimeMillis() {
    return millis;
  }
}
