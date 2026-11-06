package org.hadooptraffic.spark;

import org.apache.spark.sql.SparkSession;

/** Where the jobs register their output. */
public interface Catalog {
  /** Creates the database and all tables if missing. */
  void ensureTables();

  /** Registers one partition at an explicit location (idempotent). */
  void addPartition(String table, String dt, String location);

  /** Path-only runs: nothing is registered. */
  Catalog NONE = new Catalog() {
    @Override
    public void ensureTables() {
    }

    @Override
    public void addPartition(String table, String dt, String location) {
    }
  };

  static Catalog of(JobOptions o, SparkSession spark) {
    return o.catalog() == JobOptions.CatalogKind.HIVE
        ? new HiveCatalog(spark, o.db(), o.root()) : NONE;
  }
}
