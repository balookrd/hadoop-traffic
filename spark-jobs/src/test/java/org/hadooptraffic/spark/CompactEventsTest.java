package org.hadooptraffic.spark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class CompactEventsTest {
  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  static final String LINE_1 = "{\"ts\":1762430000123,\"start\":1762429990000,\"dn\":\"dn1\","
      + "\"op_id\":\"abc123-7\",\"user\":\"alice\",\"op\":\"read\",\"role\":\"client\","
      + "\"locality\":\"remote\",\"bpid\":\"BP-1\",\"block_id\":1073741901,"
      + "\"file_block_id\":1073741901,\"bytes\":64,\"est_bytes\":0,\"dur_ms\":10123,"
      + "\"last\":false,\"peer\":\"/10.0.0.5:51234\",\"path\":null}";
  static final String LINE_2 = "{\"ts\":1762430001000,\"start\":1762429990000,\"dn\":\"dn2\","
      + "\"op_id\":\"abc123-8\",\"user\":\"bob\",\"op\":\"webhdfs_create\",\"role\":\"client\","
      + "\"locality\":\"local\",\"bpid\":null,\"block_id\":-1,\"file_block_id\":-1,"
      + "\"bytes\":9,\"est_bytes\":0,\"dur_ms\":5,\"last\":true,\"peer\":null,"
      + "\"path\":\"/traffic/web.bin\"}";

  private String root() {
    return tmp.getRoot().toURI().toString().replaceAll("/$", "");
  }

  private void raw(String dt, String dn, String name, String... lines) throws Exception {
    File dir = new File(tmp.getRoot(), "raw/dt=" + dt + "/dn=" + dn);
    dir.mkdirs();
    Files.write(new File(dir, name).toPath(),
        (String.join("\n", lines) + "\n").getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void compactsTypedZlibOrcAndSkipsMalformedAndInFlightFiles() throws Exception {
    raw("2026-11-06", "dn1", "events-20261106-101500-000000.jsonl", LINE_1, "{not json");
    raw("2026-11-06", "dn2", "events-20261106-101600-000000.jsonl", LINE_2, "");
    raw("2026-11-06", "dn2", "events-20261106-101700-000000.jsonl._COPYING_", LINE_1);
    SparkSession spark = LocalSpark.session();
    Layout layout = new Layout(root());

    CompactEvents.Result r = CompactEvents.run(spark, layout, "2026-11-06", Catalog.NONE);

    assertEquals(2L, r.written);
    assertEquals(1L, r.malformed);
    // deploy/daily-report.sh -K deletes a raw partition only when this marker exists.
    assertTrue(new File(tmp.getRoot(), "events/dt=2026-11-06/_SUCCESS").isFile());
    List<Row> rows = spark.read().orc(layout.events("2026-11-06"))
        .orderBy("ts").collectAsList();
    assertEquals(2, rows.size());
    Row a = rows.get(0);
    assertEquals(1762430000123L, a.getLong(a.fieldIndex("ts")));
    assertEquals("alice", a.getString(a.fieldIndex("user")));
    assertEquals(1073741901L, a.getLong(a.fieldIndex("file_block_id")));
    assertEquals(false, a.getBoolean(a.fieldIndex("last")));
    assertTrue(a.isNullAt(a.fieldIndex("path")));
    Row b = rows.get(1);
    assertEquals("/traffic/web.bin", b.getString(b.fieldIndex("path")));
    assertEquals(-1L, b.getLong(b.fieldIndex("block_id")));
    assertEquals(Arrays.asList(EventSchema.SCHEMA.fieldNames()),
        Arrays.asList(spark.read().orc(layout.events("2026-11-06")).schema().fieldNames()));
    assertEquals("ZLIB",
        LocalSpark.orcCompression(new File(tmp.getRoot(), "events/dt=2026-11-06").getPath()));
  }

  @Test
  public void rerunReplacesThePartition() throws Exception {
    raw("2026-11-07", "dn1", "events-20261107-000000-000000.jsonl", LINE_1, LINE_2);
    SparkSession spark = LocalSpark.session();
    Layout layout = new Layout(root());
    CompactEvents.run(spark, layout, "2026-11-07", Catalog.NONE);
    CompactEvents.Result again = CompactEvents.run(spark, layout, "2026-11-07", Catalog.NONE);
    assertEquals(2L, again.written);
    assertEquals(2L, spark.read().orc(layout.events("2026-11-07")).count());
  }

  @Test
  public void missingRawPartitionWritesNothingAndFailsClearly() {
    try {
      CompactEvents.run(LocalSpark.session(), new Layout(root()), "2026-01-01", Catalog.NONE);
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("no raw events"));
      return;
    }
    throw new AssertionError("expected IllegalStateException");
  }

  @Test
  public void rowsWithoutTsOrOpIdAreMalformed() throws Exception {
    raw("2026-11-08", "dn1", "events-20261108-000000-000000.jsonl", LINE_1, "{}");
    SparkSession spark = LocalSpark.session();
    CompactEvents.Result r = CompactEvents.run(spark, new Layout(root()), "2026-11-08",
        Catalog.NONE);
    assertEquals(1L, r.written);
    assertEquals(1L, r.malformed);
  }

  @Test
  public void allMalformedFailsAndKeepsExistingPartition() throws Exception {
    raw("2026-11-09", "dn1", "events-20261109-000000-000000.jsonl", LINE_1, LINE_2);
    SparkSession spark = LocalSpark.session();
    Layout layout = new Layout(root());
    CompactEvents.run(spark, layout, "2026-11-09", Catalog.NONE);
    // The raw day is replaced by garbage only.
    new File(tmp.getRoot(), "raw/dt=2026-11-09/dn=dn1/events-20261109-000000-000000.jsonl")
        .delete();
    raw("2026-11-09", "dn1", "events-20261109-010000-000000.jsonl", "{not json", "{}");
    try {
      CompactEvents.run(spark, layout, "2026-11-09", Catalog.NONE);
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("all"));
      assertTrue(e.getMessage(), e.getMessage().contains("malformed"));
      assertEquals(2L, spark.read().orc(layout.events("2026-11-09")).count());
      return;
    }
    throw new AssertionError("expected IllegalStateException");
  }
}
