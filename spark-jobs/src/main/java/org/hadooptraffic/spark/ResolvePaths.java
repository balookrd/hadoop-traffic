package org.hadooptraffic.spark;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.greatest;
import static org.apache.spark.sql.functions.lag;
import static org.apache.spark.sql.functions.least;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.when;

import java.time.LocalDate;
import java.time.ZoneOffset;
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
import org.apache.spark.sql.expressions.Window;
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
    spark.udf().register("peer_ip", (UDF1<String, String>) PeerIp::of, DataTypes.StringType);

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
        // Only remote peers: a local peer is this DataNode's own host or a domain socket.
        .withColumn("peer_ip", when(col("locality").equalTo(lit("remote")),
            callUDF("peer_ip", col("peer"))).otherwise(lit(null).cast("string")))
        .localCheckpoint();

    String eventPathDir = layout.eventPath(dt);
    eventPath.write().mode(SaveMode.Overwrite).option("compression", "zlib").orc(eventPathDir);
    catalog.addPartition(Ddl.EVENT_PATH, dt, eventPathDir);

    Dataset<Row> minutes = withBusyMs(eventPath, dayStartMillis(dt))
        .withColumn("minute", col("ts").divide(60_000L).cast("long").multiply(60_000L))
        .groupBy("minute", "user", "op", "role", "locality", "table_dir")
        .agg(sum("bytes").as("bytes"), sum("est_bytes").as("est_bytes"),
            sum(when(col("last"), 1L).otherwise(0L)).as("ops"),
            sum("busy_ms").as("busy_ms"));
    String trafficDir = layout.trafficMinute(dt);
    minutes.write().mode(SaveMode.Overwrite).option("compression", "zlib").orc(trafficDir);
    catalog.addPartition(Ddl.TRAFFIC_MINUTE, dt, trafficDir);

    long total = eventPath.count();
    long unresolved = eventPath.filter(col("path").equalTo(lit(TableDir.UNKNOWN))).count();
    return new Result(image, total, unresolved);
  }

  /**
   * Adds busy_ms: the growth of dur_ms since the previous event of the same operation
   * (dn, op_id), i.e. how long the operation ran up to this event. Like bytes, it is assigned
   * to the minute of the event's ts. The first event of an operation on day D counts from the
   * operation's start, but not from before 00:00 UTC of D: the earlier part was counted on D-1.
   */
  static Dataset<Row> withBusyMs(Dataset<Row> events, long dayStart) {
    Column prev = lag(col("dur_ms"), 1)
        .over(Window.partitionBy("dn", "op_id").orderBy(col("ts"), col("dur_ms")));
    Column first = least(col("dur_ms"), col("ts").minus(lit(dayStart)));
    return events.withColumn("busy_ms",
        greatest(lit(0L), when(prev.isNull(), first).otherwise(col("dur_ms").minus(prev))));
  }

  static long dayStartMillis(String dt) {
    return LocalDate.parse(dt).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
  }

  private static FileSystem fs(SparkSession spark, String uri) {
    try {
      return new Path(uri).getFileSystem(spark.sparkContext().hadoopConfiguration());
    } catch (java.io.IOException e) {
      throw new IllegalStateException("cannot open " + uri + ": " + e, e);
    }
  }
}
