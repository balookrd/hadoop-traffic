package org.hadooptraffic.context;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.junit.Test;

public class ArgsTest {
  @Test
  public void findsFirstInstanceByType() {
    DatanodeInfo[] targets = new DatanodeInfo[2];
    Object[] args = {"a", 3, null, targets, "b", 7};
    assertEquals("a", Args.first(args, String.class));
    assertEquals(Integer.valueOf(3), Args.first(args, Integer.class));
    assertSame(targets, Args.first(args, DatanodeInfo[].class));
  }

  @Test
  public void returnsNullWhenAbsent() {
    assertNull(Args.first(new Object[] {"a"}, Long.class));
    assertNull(Args.first(null, String.class));
  }
}
