package org.hadooptraffic.model;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Operation ids unique across DataNode restarts: a random per-process prefix plus a counter.
 * The data path takes only the counter ({@link #nextSeq()}); the string is built later.
 */
public final class OpIds {
  private static final String PREFIX = String.format("%06x", new Random().nextInt(1 << 24));
  private static final AtomicLong COUNTER = new AtomicLong();

  private OpIds() {
  }

  /** Next sequence number, always > 0 (0 means "no id"). */
  public static long nextSeq() {
    return COUNTER.incrementAndGet();
  }

  /** The printable id of a sequence number: {@code <6 hex>-<seq>}. */
  public static String format(long seq) {
    return PREFIX + "-" + seq;
  }
}
