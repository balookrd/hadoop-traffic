package org.hadooptraffic.spark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ResolvePathsTest {
  static final String DT = "2026-11-06";
  static final String IMG = "2026-11-07";
  static final long DAY = 1_793_923_200_000L;     // 2026-11-06 00:00 UTC
  static final long T = DAY + 12 * 3_600_000L;    // a minute start (UTC) on DT

  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  private SparkSession spark;
  private Layout layout;

  private void orc(List<Row> rows, StructType schema, String dir) {
    spark.createDataFrame(rows, schema).coalesce(1).write().option("compression", "zlib")
        .orc(dir);
  }

  /** Model in block-mapper's schemas: id, parent, type, name, blocks. */
  private void image() {
    List<Row> inode = new ArrayList<>();
    List<Row> edge = new ArrayList<>();
    List<Row> blocks = new ArrayList<>();
    Object[][] tree = {
        {16385L, 0L, "D", ""},
        {16386L, 16385L, "D", "warehouse"},
        {16387L, 16386L, "D", "db1"},
        {16388L, 16387L, "D", "t1"},
        {16389L, 16388L, "D", "dt=2026-11-06"},
        {16390L, 16389L, "F", "part-0"},
        {16391L, 16386L, "D", "extra"},
        {16392L, 16391L, "D", "db2"},
        {16393L, 16392L, "D", "t2"},
        {16394L, 16393L, "D", "dt=2026-11-06"},
        {16395L, 16394L, "F", "000000_0"},
        {16396L, 16387L, "D", "t3"},
        {16397L, 16396L, "D", ".hive-staging_hive_1"},
        {16398L, 16397L, "D", "-ext-10000"},
        {16399L, 16398L, "F", "000000_0"},
        {16400L, 16387L, "D", "t4"},
        {16401L, 16400L, "D", "delta_0000005_0000005"},
        {16402L, 16401L, "F", "bucket_00000"},
        {16403L, 0L, "F", "gone"}};
    for (Object[] n : tree) {
      inode.add(RowFactory.create(n[0], n[2], n[3]));
      if ((Long) n[1] != 0L) {
        edge.add(RowFactory.create(n[0], n[1]));
      }
    }
    long[][] b = {{1001, 16390}, {1002, 16390}, {1003, 16395}, {1004, 16399}, {1005, 16402},
        {1006, 16403}, {Long.MIN_VALUE + 160, 16395}};
    for (long[] x : b) {
      blocks.add(RowFactory.create(x[0], x[1]));
    }
    orc(inode, PathResolverTest.INODE, layout.fsimageTable(IMG, "inode"));
    orc(edge, PathResolverTest.EDGE, layout.fsimageTable(IMG, "edge"));
    orc(blocks, new StructType().add("block_id", DataTypes.LongType)
        .add("inode_id", DataTypes.LongType), layout.fsimageTable(IMG, "block_inode"));
    List<Row> meta = new ArrayList<>();
    meta.add(RowFactory.create(42L, "fsimage_42", 0L));
    orc(meta, new StructType().add("txid", DataTypes.LongType)
        .add("image_file", DataTypes.StringType).add("parsed_at", DataTypes.LongType),
        layout.fsimageTable(IMG, "meta"));
  }

  private static Row ev(long ts, String user, String op, long block, long bytes, boolean last,
      String path) {
    return ev(ts, user, op, "remote", "/10.0.0.1:1", block, bytes, last, path);
  }

  private static Row ev(long ts, String user, String op, String locality, String peer,
      long block, long bytes, boolean last, String path) {
    return RowFactory.create(ts, ts - 1000, "dn1", "p-" + ts, user, op, "client", locality,
        block == -1 ? null : "BP-1", block, block, bytes, 0L, 1000L, last, peer, path);
  }

  /** One event of a multi-event operation: op_id, start and dur_ms are explicit. */
  private static Row op(String opId, long start, long ts, String user, long bytes,
      boolean last) {
    return RowFactory.create(ts, start, "dn1", opId, user, "write", "client", "remote", "BP-1",
        1001L, 1001L, bytes, 0L, ts - start, last, "/10.0.0.1:1", null);
  }

  private void events() {
    List<Row> rows = new ArrayList<>();
    rows.add(ev(T + 1_000, "alice", "write", 1001, 100, false, null));
    rows.add(ev(T + 70_000, "alice", "write", 1001, 50, true, null));   // next minute
    rows.add(ev(T + 2_000, "alice", "write", 1002, 10, true, null));
    rows.add(ev(T + 3_000, "bob", "write", 1003, 30, true, null));
    rows.add(ev(T + 3_500, "bob", "read", Long.MIN_VALUE + 160, 8, true, null)); // EC group
    rows.add(ev(T + 4_000, "carol", "write", 1004, 40, true, null));
    rows.add(ev(T + 5_000, "dave", "write", 1005, 5, true, null));
    rows.add(ev(T + 6_000, "erin", "write", 1006, 7, true, null));
    rows.add(ev(T + 7_000, "frank", "webhdfs_create", -1, 9, true, "/traffic/web.bin"));
    rows.add(ev(T + 8_000, "grace", "read", 9999, 3, true, null));
    rows.add(ev(T + 9_000, "heidi", "scr_fds", "local", "unix:/var/lib/hadoop-hdfs/dn_socket",
        1001, 0, true, null));
    rows.add(ev(T + 9_500, "ivan", "read", "local", "/10.0.0.9:2", 1001, 4, true, null));
    rows.add(ev(T + 9_800, "judy", "transfer", "remote", "10.0.0.3:50010", 1001, 6, true, null));
    // kate: one operation over two minutes; busy_ms is the growth of dur_ms per event.
    rows.add(op("k-1", T + 50_000, T + 55_000, "kate", 20, false));
    rows.add(op("k-1", T + 50_000, T + 65_000, "kate", 30, true));
    // liam: started on the previous day; only the part after 00:00 UTC counts on DT.
    rows.add(op("l-1", DAY - 8_000, DAY + 2_000, "liam", 5, true));
    orc(rows, EventSchema.SCHEMA, layout.events(DT));
  }

  @Before
  public void setUp() {
    spark = LocalSpark.session();
    layout = new Layout(tmp.getRoot().toURI().toString().replaceAll("/$", ""));
    image();
    events();
  }

  private Map<String, String> pathsByUser() {
    Map<String, String> m = new HashMap<>();
    for (Row r : spark.read().orc(layout.eventPath(DT)).collectAsList()) {
      m.put(r.getString(r.fieldIndex("user")) + "/" + r.getLong(r.fieldIndex("ts")),
          r.getString(r.fieldIndex("path")) + "|" + r.getString(r.fieldIndex("table_dir")));
    }
    return m;
  }

  @Test
  public void resolvesPathsAndTableDirs() throws Exception {
    ResolvePaths.Result r = ResolvePaths.run(spark, layout, DT, null, Catalog.NONE);
    assertEquals(IMG, r.imageDt);
    assertEquals(16L, r.events);
    assertEquals(2L, r.unresolved);  // erin (snapshot-only inode) and grace (unknown block)

    Map<String, String> m = pathsByUser();
    assertEquals("/warehouse/db1/t1/dt=2026-11-06/part-0|/warehouse/db1/t1",
        m.get("alice/" + (T + 1_000)));
    assertEquals("/warehouse/extra/db2/t2/dt=2026-11-06/000000_0|/warehouse/extra/db2/t2",
        m.get("bob/" + (T + 3_000)));
    assertEquals("/warehouse/extra/db2/t2/dt=2026-11-06/000000_0|/warehouse/extra/db2/t2",
        m.get("bob/" + (T + 3_500)));
    assertEquals("/warehouse/db1/t3/.hive-staging_hive_1/-ext-10000/000000_0|/warehouse/db1/t3",
        m.get("carol/" + (T + 4_000)));
    assertEquals("/warehouse/db1/t4/delta_0000005_0000005/bucket_00000|/warehouse/db1/t4",
        m.get("dave/" + (T + 5_000)));
    assertEquals("?|?", m.get("erin/" + (T + 6_000)));
    assertEquals("/traffic/web.bin|/traffic", m.get("frank/" + (T + 7_000)));
    assertEquals("?|?", m.get("grace/" + (T + 8_000)));
    assertEquals("ZLIB", LocalSpark.orcCompressionOfDir(layout.eventPath(DT)));
  }

  @Test
  public void peerIpOnlyForRemotePeers() {
    ResolvePaths.run(spark, layout, DT, IMG, Catalog.NONE);
    Map<String, String> m = new HashMap<>();
    for (Row r : spark.read().orc(layout.eventPath(DT)).collectAsList()) {
      m.put(r.getString(r.fieldIndex("user")), r.getString(r.fieldIndex("peer_ip")));
    }
    assertEquals("10.0.0.1", m.get("alice"));
    assertEquals("10.0.0.3", m.get("judy"));     // DataNode transfer: "ip:port"
    assertNull(m.get("heidi"));                  // domain socket
    assertNull(m.get("ivan"));                   // local TCP peer
    assertTrue(m.containsKey("ivan"));
  }

  @Test
  public void trafficMinuteAggregates() {
    ResolvePaths.run(spark, layout, DT, IMG, Catalog.NONE);
    Map<String, long[]> m = new HashMap<>();
    for (Row r : spark.read().orc(layout.trafficMinute(DT)).collectAsList()) {
      m.put(r.getLong(r.fieldIndex("minute")) + "|" + r.getString(r.fieldIndex("user")) + "|"
              + r.getString(r.fieldIndex("op")) + "|" + r.getString(r.fieldIndex("table_dir")),
          new long[] {r.getLong(r.fieldIndex("bytes")), r.getLong(r.fieldIndex("ops")),
              r.getLong(r.fieldIndex("busy_ms"))});
    }
    // alice: minute T has 100 (not last) + 10 (last) bytes, 1 op; minute T+60s has 50, 1 op.
    assertEquals(110L, m.get(T + "|alice|write|/warehouse/db1/t1")[0]);
    assertEquals(1L, m.get(T + "|alice|write|/warehouse/db1/t1")[1]);
    assertEquals(50L, m.get((T + 60_000) + "|alice|write|/warehouse/db1/t1")[0]);
    assertEquals(1L, m.get((T + 60_000) + "|alice|write|/warehouse/db1/t1")[1]);
    assertEquals(40L, m.get(T + "|carol|write|/warehouse/db1/t3")[0]);
    assertEquals(7L, m.get(T + "|erin|write|?")[0]);
    assertEquals(9L, m.get(T + "|frank|webhdfs_create|/traffic")[0]);
    // Single-event operations: busy_ms is dur_ms (1000) per event.
    assertEquals(2000L, m.get(T + "|alice|write|/warehouse/db1/t1")[2]);
    assertEquals(1000L, m.get((T + 60_000) + "|alice|write|/warehouse/db1/t1")[2]);
    // kate: 5 s up to the first event (minute T), 10 s more up to the last (minute T+60s).
    assertEquals(5000L, m.get(T + "|kate|write|/warehouse/db1/t1")[2]);
    assertEquals(0L, m.get(T + "|kate|write|/warehouse/db1/t1")[1]);
    assertEquals(10000L, m.get((T + 60_000) + "|kate|write|/warehouse/db1/t1")[2]);
    assertEquals(1L, m.get((T + 60_000) + "|kate|write|/warehouse/db1/t1")[1]);
    // liam: dur_ms 10 s, but only 2 s fall on DT.
    assertEquals(2000L, m.get(DAY + "|liam|write|/warehouse/db1/t1")[2]);
    assertEquals(new java.util.HashSet<>(java.util.Arrays.asList("minute", "user", "op", "role",
        "locality", "table_dir", "bytes", "est_bytes", "ops", "busy_ms", "origin")),
        new java.util.HashSet<>(java.util.Arrays.asList(
            spark.read().orc(layout.trafficMinute(DT)).schema().fieldNames())));
  }

  private Map<String, String> originByUser() {
    Map<String, String> m = new HashMap<>();
    for (Row r : spark.read().orc(layout.trafficMinute(DT)).collectAsList()) {
      m.put(r.getString(r.fieldIndex("user")), r.getString(r.fieldIndex("origin")));
    }
    return m;
  }

  @Test
  public void originFromNets() {
    // alice's peer is 10.0.0.1, judy's 10.0.0.3; heidi (domain socket) and ivan are local.
    ResolvePaths.run(spark, layout, DT, IMG, Catalog.NONE, Nets.parse("own=10.0.0.0/31"));
    Map<String, String> m = originByUser();
    assertEquals("own", m.get("alice"));
    assertEquals(Nets.OTHER, m.get("judy"));
    assertEquals(Nets.LOCAL, m.get("heidi"));
    assertEquals(Nets.LOCAL, m.get("ivan"));
  }

  @Test
  public void originWithoutNets() {
    ResolvePaths.run(spark, layout, DT, IMG, Catalog.NONE);
    Map<String, String> m = originByUser();
    assertEquals(Nets.OTHER, m.get("alice"));
    assertEquals(Nets.LOCAL, m.get("ivan"));
  }

  @Test
  public void rerunIsIdempotent() {
    ResolvePaths.run(spark, layout, DT, IMG, Catalog.NONE);
    long first = spark.read().orc(layout.trafficMinute(DT)).count();
    ResolvePaths.run(spark, layout, DT, IMG, Catalog.NONE);
    assertEquals(first, spark.read().orc(layout.trafficMinute(DT)).count());
    assertEquals(16L, spark.read().orc(layout.eventPath(DT)).count());
  }
}
