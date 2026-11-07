package org.hadooptraffic.mapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

public class MapperOptionsTest {
  @Test
  public void parsesRequiredOptionsWithOrcDefault() {
    MapperOptions o = MapperOptions.parse(
        new String[] {"-i", "/data/fsimg/fsimage_0000000000000000042", "-o", "hdfs:///x/dt=2026-11-06"});
    assertEquals("/data/fsimg/fsimage_0000000000000000042", o.image());
    assertEquals("hdfs:///x/dt=2026-11-06", o.output());
    assertEquals(MapperOptions.Format.ORC, o.format());
  }

  @Test
  public void parsesTsvFormat() {
    assertEquals(MapperOptions.Format.TSV,
        MapperOptions.parse(new String[] {"-i", "a", "-o", "b", "--format", "tsv"}).format());
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsMissingInput() {
    MapperOptions.parse(new String[] {"-o", "b"});
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsMissingOutput() {
    MapperOptions.parse(new String[] {"-i", "a"});
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsUnknownFormat() {
    MapperOptions.parse(new String[] {"-i", "a", "-o", "b", "--format", "parquet"});
  }

  @Test
  public void rejectsUnknownOption() {
    try {
      MapperOptions.parse(new String[] {"-x", "v", "-i", "a", "-o", "b"});
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("unknown option: -x"));
    }
  }

  @Test
  public void threadsDefaultAndOverride() {
    assertEquals(MapperOptions.DEFAULT_THREADS,
        MapperOptions.parse(new String[] {"-i", "a", "-o", "b"}).threads());
    assertEquals(4,
        MapperOptions.parse(new String[] {"-i", "a", "-o", "b", "--threads", "4"}).threads());
  }

  @Test
  public void rejectsBadThreads() {
    for (String v : new String[] {"0", "65", "x", "-1"}) {
      try {
        MapperOptions.parse(new String[] {"-i", "a", "-o", "b", "--threads", v});
        fail("expected IllegalArgumentException for " + v);
      } catch (IllegalArgumentException e) {
        assertTrue(e.getMessage(), e.getMessage().contains("--threads"));
      }
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsOptionWithoutValue() {
    MapperOptions.parse(new String[] {"-i", "a", "-o"});
  }
}
