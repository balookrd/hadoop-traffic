package org.hadooptraffic.context;

/** Time source; replaceable in tests. */
public interface Clock {
  Clock SYSTEM = new Clock() {
    @Override
    public long nanoTime() {
      return System.nanoTime();
    }

    @Override
    public long currentTimeMillis() {
      return System.currentTimeMillis();
    }
  };

  long nanoTime();

  long currentTimeMillis();
}
