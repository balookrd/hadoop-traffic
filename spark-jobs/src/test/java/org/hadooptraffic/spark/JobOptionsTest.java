package org.hadooptraffic.spark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class JobOptionsTest {
  @Test
  public void defaults() {
    JobOptions o = JobOptions.parse(new String[] {"--dt", "2026-11-06"});
    assertEquals("2026-11-06", o.dt());
    assertNull(o.imageDt());
    assertEquals("/data/hdfs-traffic", o.root());
    assertEquals("hdfs_traffic", o.db());
    assertEquals(JobOptions.CatalogKind.HIVE, o.catalog());
  }

  @Test
  public void allOptions() {
    JobOptions o = JobOptions.parse(new String[] {"--dt", "2026-11-06", "--image-dt",
        "2026-11-07", "--root", "hdfs:///x", "--db", "traffic", "--catalog", "none"});
    assertEquals("2026-11-07", o.imageDt());
    assertEquals("hdfs:///x", o.root());
    assertEquals("traffic", o.db());
    assertEquals(JobOptions.CatalogKind.NONE, o.catalog());
  }

  @Test
  public void rootTrailingSlashIsDropped() {
    assertEquals("/data/x", JobOptions.parse(new String[] {"--dt", "2026-11-06", "--root",
        "/data/x/"}).root());
  }

  private static void assertRejected(String... args) {
    try {
      JobOptions.parse(args);
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("usage:"));
      return;
    }
    throw new AssertionError("accepted: " + String.join(" ", args));
  }

  @Test
  public void rejectsBadInput() {
    assertRejected();
    assertRejected("--dt", "2026-11-6");
    assertRejected("--dt", "2026-11-06'; DROP TABLE x; --");
    assertRejected("--dt", "2026-11-06", "--image-dt", "yesterday");
    assertRejected("--dt", "2026-11-06", "--catalog", "glue");
    assertRejected("--dt", "2026-11-06", "--db", "bad-name");
    assertRejected("--dt", "2026-11-06", "--root", "/x'y");
    assertRejected("--dt", "2026-11-06", "--root", "/x\\y");
    assertRejected("--dt", "2026-11-06", "--root", "/x\ny");
    assertRejected("--dt", "2026-11-06", "--root", "/x\u0000y");
    assertRejected("--dt", "2026-11-06", "--root", "/");
    assertRejected("--dt", "2026-11-06", "--root", "//");
    assertRejected("--dt", "2026-11-06", "--root", "hdfs:///");
    assertRejected("--dt", "2026-11-06", "--root", "hdfs://nn:8020/");
    assertRejected("--dt", "2026-11-06", "--what", "x");
    assertRejected("--dt");
  }
}
