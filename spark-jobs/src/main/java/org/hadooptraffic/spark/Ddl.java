package org.hadooptraffic.spark;

import java.util.ArrayList;
import java.util.List;

/** Hive DDL of every table. EXTERNAL, ORC with ZLIB, partitioned by dt, explicit locations. */
public final class Ddl {
  public static final String EVENTS = "events";
  public static final String FS_INODE = "fs_inode";
  public static final String FS_EDGE = "fs_edge";
  public static final String FS_BLOCK_INODE = "fs_block_inode";
  public static final String FS_META = "fs_meta";
  public static final String EVENT_PATH = "event_path";
  public static final String TRAFFIC_MINUTE = "traffic_minute";

  /** Columns of events, in the order of the JSON lines written by the agent. */
  public static final String EVENT_COLUMNS = "`ts` BIGINT, `start` BIGINT, `dn` STRING, "
      + "`op_id` STRING, `user` STRING, `op` STRING, `role` STRING, `locality` STRING, "
      + "`bpid` STRING, `block_id` BIGINT, `file_block_id` BIGINT, `bytes` BIGINT, "
      + "`est_bytes` BIGINT, `dur_ms` BIGINT, `last` BOOLEAN, `peer` STRING, `path` STRING";

  private Ddl() {
  }

  public static List<String> statements(String db, String root) {
    List<String> s = new ArrayList<>();
    s.add("CREATE DATABASE IF NOT EXISTS " + db);
    s.add(table(db, EVENTS, EVENT_COLUMNS, root + "/events"));
    s.add(table(db, FS_INODE, "`inode_id` BIGINT, `type` STRING, `name` STRING",
        root + "/fsimage"));
    s.add(table(db, FS_EDGE, "`child_id` BIGINT, `parent_id` BIGINT", root + "/fsimage"));
    s.add(table(db, FS_BLOCK_INODE, "`block_id` BIGINT, `inode_id` BIGINT", root + "/fsimage"));
    s.add(table(db, FS_META, "`txid` BIGINT, `image_file` STRING, `parsed_at` BIGINT",
        root + "/fsimage"));
    s.add(table(db, EVENT_PATH,
        EVENT_COLUMNS + ", `inode_id` BIGINT, `table_dir` STRING, `peer_ip` STRING",
        root + "/event_path"));
    // Dimensions first, then metrics.
    s.add(table(db, TRAFFIC_MINUTE, "`minute` BIGINT, `user` STRING, `op` STRING, "
        + "`role` STRING, `locality` STRING, `origin` STRING, `table_dir` STRING, "
        + "`bytes` BIGINT, `est_bytes` BIGINT, `ops` BIGINT, `busy_ms` BIGINT",
        root + "/traffic_minute"));
    return s;
  }

  private static String table(String db, String name, String columns, String location) {
    return "CREATE EXTERNAL TABLE IF NOT EXISTS " + db + "." + name + " (" + columns + ") "
        + "PARTITIONED BY (`dt` STRING) STORED AS ORC LOCATION '" + location + "' "
        + "TBLPROPERTIES ('orc.compress'='ZLIB')";
  }

  public static String addPartition(String db, String table, String dt, String location) {
    return "ALTER TABLE " + db + "." + table + " ADD IF NOT EXISTS PARTITION (dt='" + dt + "') "
        + "LOCATION '" + location + "'";
  }

  /** The DDL as a SQL script (statements end with ";\n"), as kept in hive/schema.sql. */
  public static String render(String db, String root) {
    StringBuilder sb = new StringBuilder();
    sb.append("-- Generated from org.hadooptraffic.spark.Ddl (database ").append(db)
        .append(", root ").append(root).append(").\n")
        .append("-- Partitions are added explicitly by the jobs; never run MSCK REPAIR TABLE.\n");
    for (String s : statements(db, root)) {
      sb.append(s).append(";\n");
    }
    return sb.toString();
  }
}
