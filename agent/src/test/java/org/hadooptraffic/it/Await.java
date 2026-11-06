package org.hadooptraffic.it;

import java.util.function.BooleanSupplier;

/** Polls a condition: DN ops finish asynchronously after the client call returns. */
public final class Await {
  private Await() {
  }

  public static void until(BooleanSupplier condition, long timeoutMs)
      throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (!condition.getAsBoolean()) {
      if (System.currentTimeMillis() > deadline) {
        throw new AssertionError("Condition not met in " + timeoutMs + " ms. Counters:\n"
            + TrafficAssert.dump());
      }
      Thread.sleep(100);
    }
  }
}
