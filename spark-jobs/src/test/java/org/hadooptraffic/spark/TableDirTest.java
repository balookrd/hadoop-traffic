package org.hadooptraffic.spark;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TableDirTest {
  @Test
  public void partitionedTablesAtAnyDepth() {
    assertEquals("/data/warehouse/db/t",
        TableDir.of("/data/warehouse/db/t/dt=2026-11-06/part-0"));
    assertEquals("/data/warehouse/extra/db/t",
        TableDir.of("/data/warehouse/extra/db/t/dt=2026-11-06/hour=13/part-0"));
  }

  @Test
  public void stagingAndTemporaryDirectories() {
    assertEquals("/w/db/t",
        TableDir.of("/w/db/t/.hive-staging_hive_2026-11-06_1/-ext-10000/dt=2026-11-06/000000_0"));
    assertEquals("/w/db/t", TableDir.of("/w/db/t/.spark-staging-1a2b/dt=x/part-0"));
    assertEquals("/w/db/t",
        TableDir.of("/w/db/t/_temporary/0/_temporary/attempt_1_0001_m_000000_0/part-0"));
  }

  @Test
  public void hiveAcidDirectories() {
    assertEquals("/w/db.db/t", TableDir.of("/w/db.db/t/delta_0000005_0000005/bucket_00000"));
    assertEquals("/w/db.db/t", TableDir.of("/w/db.db/t/delta_0000005_0000005_0000/bucket_0"));
    assertEquals("/w/db.db/t", TableDir.of("/w/db.db/t/delete_delta_0000007_0000007/bucket_0"));
    assertEquals("/w/db.db/t", TableDir.of("/w/db.db/t/base_0000010_v0000123/bucket_00000"));
    assertEquals("/w/db.db/t",
        TableDir.of("/w/db.db/t/p=1/delta_0000005_0000005/bucket_00000"));
  }

  @Test
  public void unpartitionedTableIsTheParentDirectory() {
    assertEquals("/w/db/t", TableDir.of("/w/db/t/000000_0"));
    assertEquals("/traffic", TableDir.of("/traffic/alice.bin"));
  }

  @Test
  public void namesThatOnlyLookSpecialAreKept() {
    assertEquals("/w/basement/deltas/t", TableDir.of("/w/basement/deltas/t/f"));
    assertEquals("/w/db/base_x/t", TableDir.of("/w/db/base_x/t/f"));
  }

  @Test
  public void edgeCases() {
    assertEquals("/", TableDir.of("/file-in-root"));
    assertEquals("?", TableDir.of("?"));
    assertEquals("?", TableDir.of(null));
    assertEquals("?", TableDir.of(""));
    assertEquals("?", TableDir.of("relative/path"));
    assertEquals("/", TableDir.of("/dt=2026-11-06/f"));
  }
}
