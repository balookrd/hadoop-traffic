package org.hadooptraffic.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class OpIdsTest {
  @Test
  public void sequenceNumbersArePositiveAndIncreasing() {
    long a = OpIds.nextSeq();
    long b = OpIds.nextSeq();
    assertTrue(a > 0);
    assertTrue(b > a);
  }

  @Test
  public void formatSharesProcessPrefix() {
    String a = OpIds.format(7L);
    String b = OpIds.format(8L);
    assertTrue(a, a.matches("[0-9a-f]{6}-7"));
    assertEquals(a.substring(0, 7), b.substring(0, 7));
  }
}
