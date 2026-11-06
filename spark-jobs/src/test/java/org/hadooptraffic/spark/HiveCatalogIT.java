package org.hadooptraffic.spark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class HiveCatalogIT {
  private static SparkSession spark;
  private static File root;

  @BeforeClass
  public static void start() throws Exception {
    System.setProperty("derby.stream.error.file",
        new File("target/derby.log").getAbsolutePath());
    File base = new File("target/hive-it").getAbsoluteFile();
    root = new File(base, "data");
    spark = SparkSession.builder().master("local[1]").appName("HiveCatalogIT")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.warehouse.dir", new File(base, "warehouse").toURI().toString())
        .config("spark.hadoop.javax.jdo.option.ConnectionURL",
            "jdbc:derby:;databaseName=" + new File(base, "metastore_db").getPath() + ";create=true")
        .enableHiveSupport()
        .getOrCreate();
  }

  @AfterClass
  public static void stop() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  public void createsTablesAndRegistersPartitionsIdempotently() {
    HiveCatalog c = new HiveCatalog(spark, "hdfs_traffic_it", root.toURI().toString());
    c.ensureTables();
    c.ensureTables();
    String loc = new File(root, "fsimage/dt=2026-11-07/edge").toURI().toString();
    c.addPartition(Ddl.FS_EDGE, "2026-11-07", loc);
    c.addPartition(Ddl.FS_EDGE, "2026-11-07", loc);

    List<Row> tables = spark.sql("SHOW TABLES IN hdfs_traffic_it").collectAsList();
    assertEquals(7, tables.size());
    List<Row> parts = spark.sql("SHOW PARTITIONS hdfs_traffic_it.fs_edge").collectAsList();
    assertEquals(1, parts.size());
    assertEquals("dt=2026-11-07", parts.get(0).getString(0));
    String ddl = spark.sql("SHOW CREATE TABLE hdfs_traffic_it.events AS SERDE")
        .collectAsList().get(0).getString(0);
    assertTrue(ddl, ddl.contains("EXTERNAL"));
    assertTrue(ddl, ddl.contains("OrcSerde"));
  }
}
