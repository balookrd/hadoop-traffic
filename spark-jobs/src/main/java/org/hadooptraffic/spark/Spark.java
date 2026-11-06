package org.hadooptraffic.spark;

import org.apache.spark.sql.SparkSession;

/** Session for spark-submit: master and resources come from the command line. */
final class Spark {
  private Spark() {
  }

  static SparkSession session(String app, JobOptions o) {
    SparkSession.Builder b = SparkSession.builder().appName(app);
    if (o.catalog() == JobOptions.CatalogKind.HIVE) {
      b.enableHiveSupport();
    }
    return b.getOrCreate();
  }
}
