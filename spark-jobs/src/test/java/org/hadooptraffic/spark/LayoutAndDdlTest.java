package org.hadooptraffic.spark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.junit.Test;

public class LayoutAndDdlTest {
  @Test
  public void layout() {
    Layout l = new Layout("hdfs:///data/hdfs-traffic");
    assertEquals("hdfs:///data/hdfs-traffic/raw/dt=2026-11-06", l.raw("2026-11-06"));
    assertEquals("hdfs:///data/hdfs-traffic/events/dt=2026-11-06", l.events("2026-11-06"));
    assertEquals("hdfs:///data/hdfs-traffic/fsimage", l.fsimageRoot());
    assertEquals("hdfs:///data/hdfs-traffic/fsimage/dt=2026-11-07/block_inode",
        l.fsimageTable("2026-11-07", "block_inode"));
    assertEquals("hdfs:///data/hdfs-traffic/event_path/dt=2026-11-06", l.eventPath("2026-11-06"));
    assertEquals("hdfs:///data/hdfs-traffic/traffic_minute/dt=2026-11-06",
        l.trafficMinute("2026-11-06"));
  }

  @Test
  public void ddlCoversEveryTableAsExternalZlibOrc() {
    List<String> s = Ddl.statements("hdfs_traffic", "/data/hdfs-traffic");
    assertEquals(8, s.size());
    assertEquals("CREATE DATABASE IF NOT EXISTS hdfs_traffic", s.get(0));
    for (String t : s.subList(1, s.size())) {
      assertTrue(t, t.startsWith("CREATE EXTERNAL TABLE IF NOT EXISTS hdfs_traffic."));
      assertTrue(t, t.contains("PARTITIONED BY (`dt` STRING) STORED AS ORC LOCATION '"));
      assertTrue(t, t.endsWith("TBLPROPERTIES ('orc.compress'='ZLIB')"));
    }
    assertTrue(s.get(2).contains("hdfs_traffic.fs_inode (`inode_id` BIGINT, `type` STRING, "
        + "`name` STRING)"));
    assertTrue(s.get(2).contains("LOCATION '/data/hdfs-traffic/fsimage'"));
  }

  @Test
  public void addPartitionStatement() {
    assertEquals("ALTER TABLE hdfs_traffic.fs_edge ADD IF NOT EXISTS PARTITION (dt='2026-11-07') "
        + "LOCATION '/r/fsimage/dt=2026-11-07/edge'",
        Ddl.addPartition("hdfs_traffic", "fs_edge", "2026-11-07", "/r/fsimage/dt=2026-11-07/edge"));
  }

  /** hive/schema.sql is the operator's copy of the DDL; it must not drift from the code. */
  @Test
  public void schemaSqlMatchesTheCode() throws Exception {
    String file = new String(Files.readAllBytes(Paths.get("..", "hive", "schema.sql")),
        StandardCharsets.UTF_8);
    assertEquals(Ddl.render("hdfs_traffic", "/data/hdfs-traffic"), file);
  }
}
