package org.hadooptraffic.spark;

import org.apache.spark.sql.SparkSession;

/** Registers tables and partitions in the Hive metastore through Spark SQL. */
public final class HiveCatalog implements Catalog {
  private final SparkSession spark;
  private final String db;
  private final String root;

  public HiveCatalog(SparkSession spark, String db, String root) {
    this.spark = spark;
    this.db = db;
    this.root = root;
  }

  @Override
  public void ensureTables() {
    for (String s : Ddl.statements(db, root)) {
      spark.sql(s);
    }
  }

  @Override
  public void addPartition(String table, String dt, String location) {
    spark.sql(Ddl.addPartition(db, table, dt, location));
  }
}
