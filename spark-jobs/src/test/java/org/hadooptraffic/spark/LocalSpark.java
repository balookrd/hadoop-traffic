package org.hadooptraffic.spark;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.spark.sql.SparkSession;

/** One local SparkSession per test JVM. */
final class LocalSpark {
  private static SparkSession spark;

  private LocalSpark() {
  }

  static synchronized SparkSession session() {
    if (spark == null) {
      spark = SparkSession.builder().master("local[2]").appName("spark-jobs-test")
          .config("spark.ui.enabled", "false")
          .config("spark.sql.shuffle.partitions", "4")
          .config("spark.driver.bindAddress", "127.0.0.1")
          .config("spark.driver.host", "127.0.0.1")
          .getOrCreate();
    }
    return spark;
  }

  /** Compression kind of the first ORC data file in a local directory. */
  static String orcCompression(String dir) throws IOException {
    File[] files = new File(dir).listFiles((d, n) -> n.endsWith(".orc"));
    if (files == null || files.length == 0) {
      throw new IOException("no .orc file in " + dir);
    }
    return OrcFile.createReader(new Path(files[0].toURI()),
        OrcFile.readerOptions(new Configuration())).getCompressionKind().name();
  }

  /** Compression kind of the first ORC data file in a directory given as a file: URI. */
  static String orcCompressionOfDir(String fileUri) throws IOException {
    return orcCompression(new File(java.net.URI.create(fileUri)).getPath());
  }
}
