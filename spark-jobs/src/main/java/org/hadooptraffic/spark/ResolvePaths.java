package org.hadooptraffic.spark;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.when;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * events/dt=D + fsimage/dt=I → event_path/dt=D and traffic_minute/dt=D. The image defaults to
 * the latest complete one, so paths are current paths (files written on D are in the image of
 * D+1; renames out of staging directories are already applied).
 */
public final class ResolvePaths {
  private static final Logger LOG = LoggerFactory.getLogger(ResolvePaths.class);

  private ResolvePaths() {
  }

  /** Counts of one run. */
  public static final class Result {
    public final String imageDt;
    public final long events;
    public final long unresolved;

    Result(String imageDt, long events, long unresolved) {
      this.imageDt = imageDt;
      this.events = events;
      this.unresolved = unresolved;
    }
  }

  public static void main(String[] args) {
    JobOptions o;
    try {
      o = JobOptions.parse(args);
    } catch (IllegalArgumentException e) {
      System.err.println("ResolvePaths: " + e.getMessage());
      System.exit(2);
      return;
    }
    SparkSession spark = Spark.session("ResolvePaths " + o.dt(), o);
    try {
      Result r = run(spark, new Layout(o.root()), o.dt(), o.imageDt(), Catalog.of(o, spark));
      System.out.println("ResolvePaths: dt=" + o.dt() + " image=" + r.imageDt + " events="
          + r.events + " unresolved=" + r.unresolved);
    } finally {
      spark.stop();
    }
  }

  public static Result run(SparkSession spark, Layout layout, String dt, String imageDt,
      Catalog catalog) {
    String image = imageDt != null ? imageDt : Images.latest(fs(spark, layout.fsimageRoot()),
        new Path(layout.fsimageRoot()));
    if (image.compareTo(dt) <= 0) {
      LOG.warn("fsimage dt={} is not after dt={}: files created on {} may resolve to '?'",
          image, dt, dt);
    }
    catalog.ensureTables();
    catalog.addPartition(Ddl.FS_INODE, image, layout.fsimageTable(image, "inode"));
    catalog.addPartition(Ddl.FS_EDGE, image, layout.fsimageTable(image, "edge"));
    catalog.addPartition(Ddl.FS_BLOCK_INODE, image, layout.fsimageTable(image, "block_inode"));
    catalog.addPartition(Ddl.FS_META, image, layout.fsimageTable(image, "meta"));

    spark.udf().register("table_dir", (UDF1<String, String>) TableDir::of, DataTypes.StringType);

    Dataset<Row> events = spark.read().orc(layout.events(dt));
    Dataset<Row> blockInode = spark.read().orc(layout.fsimageTable(image, "block_inode"));
    Dataset<Row> edge = spark.read().orc(layout.fsimageTable(image, "edge"));
    Dataset<Row> inode = spark.read().orc(layout.fsimageTable(image, "inode"))
        .select("inode_id", "name");

    // Block → inode, only for blocks seen on day D.
    Dataset<Row> blocks = events.select(col("file_block_id").as("block_id"))
        .filter(col("block_id").notEqual(lit(-1L))).distinct();
    Dataset<Row> fileOf = blocks.join(blockInode, "block_id").select("block_id", "inode_id")
        .localCheckpoint();
    Dataset<Row> paths = PathResolver.resolve(fileOf.select("inode_id"), edge,
        inode.withColumn("type", lit("?")), PathResolver.DEFAULT_MAX_ITERATIONS)
        .withColumnRenamed("path", "resolved");

    Dataset<Row> e = events.alias("e");
    Dataset<Row> joined = e
        .join(fileOf.alias("f"), col("e.file_block_id").equalTo(col("f.block_id")), "left")
        .join(paths.alias("p"), col("f.inode_id").equalTo(col("p.inode_id")), "left");
    Column path = coalesce(col("e.path"), col("p.resolved"), lit(TableDir.UNKNOWN));
    List<Column> cols = new ArrayList<>();
    for (String f : EventSchema.SCHEMA.fieldNames()) {
      cols.add("path".equals(f) ? path.as("path") : col("e." + f).as(f));
    }
    cols.add(col("f.inode_id").as("inode_id"));
    Dataset<Row> eventPath = joined.select(cols.toArray(new Column[0]))
        .withColumn("table_dir", callUDF("table_dir", col("path")))
        .localCheckpoint();

    String eventPathDir = layout.eventPath(dt);
    eventPath.write().mode(SaveMode.Overwrite).option("compression", "zlib").orc(eventPathDir);
    catalog.addPartition(Ddl.EVENT_PATH, dt, eventPathDir);

    Dataset<Row> minutes = eventPath
        .withColumn("minute", col("ts").divide(60_000L).cast("long").multiply(60_000L))
        .groupBy("minute", "user", "op", "role", "locality", "table_dir")
        .agg(sum("bytes").as("bytes"), sum("est_bytes").as("est_bytes"),
            sum(when(col("last"), 1L).otherwise(0L)).as("ops"));
    String trafficDir = layout.trafficMinute(dt);
    minutes.write().mode(SaveMode.Overwrite).option("compression", "zlib").orc(trafficDir);
    catalog.addPartition(Ddl.TRAFFIC_MINUTE, dt, trafficDir);

    long total = eventPath.count();
    long unresolved = eventPath.filter(col("path").equalTo(lit(TableDir.UNKNOWN))).count();
    return new Result(image, total, unresolved);
  }

  private static FileSystem fs(SparkSession spark, String uri) {
    try {
      return new Path(uri).getFileSystem(spark.sparkContext().hadoopConfiguration());
    } catch (java.io.IOException e) {
      throw new IllegalStateException("cannot open " + uri + ": " + e, e);
    }
  }
}
