package org.hadooptraffic.spark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

public class PathResolverTest {
  static final StructType INODE = new StructType().add("inode_id", DataTypes.LongType)
      .add("type", DataTypes.StringType).add("name", DataTypes.StringType);
  static final StructType EDGE = new StructType().add("child_id", DataTypes.LongType)
      .add("parent_id", DataTypes.LongType);
  static final StructType ID = new StructType().add("inode_id", DataTypes.LongType);

  /** A tiny namespace builder: names and parent links. */
  static final class Tree {
    final List<Row> inodes = new ArrayList<>();
    final List<Row> edges = new ArrayList<>();

    Tree() {
      inodes.add(RowFactory.create(PathResolver.ROOT, "D", ""));
    }

    Tree node(long id, long parent, String type, String name) {
      inodes.add(RowFactory.create(id, type, name));
      if (parent != 0) {
        edges.add(RowFactory.create(id, parent));
      }
      return this;
    }
  }

  private static Map<Long, String> resolve(Tree t, int max, long... needed) {
    SparkSession spark = LocalSpark.session();
    List<Row> ids = new ArrayList<>();
    for (long n : needed) {
      ids.add(RowFactory.create(n));
    }
    Dataset<Row> out = PathResolver.resolve(spark.createDataFrame(ids, ID),
        spark.createDataFrame(t.edges, EDGE), spark.createDataFrame(t.inodes, INODE), max);
    Map<Long, String> m = new HashMap<>();
    for (Row r : out.collectAsList()) {
      m.put(r.getLong(0), r.isNullAt(1) ? null : r.getString(1));
    }
    assertEquals(needed.length, m.size());
    return m;
  }

  @Test
  public void resolvesShallowAndSharedAncestors() {
    Tree t = new Tree()
        .node(2, PathResolver.ROOT, "D", "data")
        .node(3, 2, "D", "a")
        .node(4, 3, "F", "f1")
        .node(5, 3, "F", "f2")
        .node(6, PathResolver.ROOT, "F", "top");
    Map<Long, String> m = resolve(t, 64, 4, 5, 6, PathResolver.ROOT);
    assertEquals("/data/a/f1", m.get(4L));
    assertEquals("/data/a/f2", m.get(5L));
    assertEquals("/top", m.get(6L));
    assertEquals("/", m.get(PathResolver.ROOT));
  }

  @Test
  public void resolvesDeepChains() {
    Tree t = new Tree();
    long parent = PathResolver.ROOT;
    StringBuilder expected = new StringBuilder();
    for (int depth = 1; depth <= 40; depth++) {
      t.node(1000 + depth, parent, depth == 40 ? "F" : "D", "d" + depth);
      expected.append("/d").append(depth);
      parent = 1000 + depth;
    }
    assertEquals(expected.toString(), resolve(t, 64, 1040).get(1040L));
  }

  @Test
  public void orphansAndCyclesStayUnresolved() {
    Tree t = new Tree()
        .node(10, 0, "F", "orphan")          // no parent edge: snapshot-only inode
        .node(11, 12, "D", "x")              // 11 -> 12 -> 11: corrupt cycle
        .node(12, 11, "D", "y")
        .node(13, 11, "F", "in-cycle")
        .node(14, 99, "F", "dangling");      // parent missing from the image
    Map<Long, String> m = resolve(t, 8, 10, 13, 14);
    assertNull(m.get(10L));
    assertNull(m.get(13L));
    assertNull(m.get(14L));
  }

  @Test
  public void neededIdsMayRepeat() {
    Tree t = new Tree().node(2, PathResolver.ROOT, "F", "f");
    SparkSession spark = LocalSpark.session();
    List<Row> ids = new ArrayList<>();
    ids.add(RowFactory.create(2L));
    ids.add(RowFactory.create(2L));
    assertEquals(1L, PathResolver.resolve(spark.createDataFrame(ids, ID),
        spark.createDataFrame(t.edges, EDGE), spark.createDataFrame(t.inodes, INODE), 64)
        .count());
  }

  private static PathResolver.Resolved stats(Tree t, int max, long... needed) {
    SparkSession spark = LocalSpark.session();
    List<Row> ids = new ArrayList<>();
    for (long n : needed) {
      ids.add(RowFactory.create(n));
    }
    return PathResolver.resolveWithStats(spark.createDataFrame(ids, ID),
        spark.createDataFrame(t.edges, EDGE), spark.createDataFrame(t.inodes, INODE), max);
  }

  private static Map<Long, String> collect(Dataset<Row> out) {
    Map<Long, String> m = new HashMap<>();
    for (Row r : out.collectAsList()) {
      m.put(r.getLong(0), r.isNullAt(1) ? null : r.getString(1));
    }
    return m;
  }

  private static Tree chain(int depth) {
    Tree t = new Tree();
    long parent = PathResolver.ROOT;
    for (int d = 1; d <= depth; d++) {
      t.node(1000 + d, parent, d == depth ? "F" : "D", "d" + d);
      parent = 1000 + d;
    }
    return t;
  }

  @Test
  public void deepChainResolvesInLogarithmicRounds() {
    PathResolver.Resolved r = stats(chain(40), 64, 1040);
    StringBuilder expected = new StringBuilder();
    for (int d = 1; d <= 40; d++) {
      expected.append("/d").append(d);
    }
    assertEquals(expected.toString(), collect(r.paths).get(1040L));
    assertTrue("jumpRounds=" + r.jumpRounds, r.jumpRounds <= maxRounds());
  }

  private static int maxRounds() {
    return 1 + (int) Math.ceil(Math.log(PathResolver.DEFAULT_MAX_ITERATIONS + 1) / Math.log(2));
  }

  @Test
  public void filesAtEveryLevelOfADeepChain() {
    Tree t = new Tree();
    long parent = PathResolver.ROOT;
    long[] needed = new long[41];
    for (int k = 1; k <= 40; k++) {
      t.node(1000 + k, parent, "D", "d" + k);
      t.node(2000 + k, 1000 + k, "F", "f" + k);
      needed[k - 1] = 2000 + k;
      parent = 1000 + k;
    }
    t.node(3000, parent, "F", "F");
    needed[40] = 3000;
    Map<Long, String> m = resolve(t, PathResolver.DEFAULT_MAX_ITERATIONS, needed);
    StringBuilder dirs = new StringBuilder();
    for (int k = 1; k <= 40; k++) {
      dirs.append("/d").append(k);
      assertEquals(dirs + "/f" + k, m.get(2000L + k));
    }
    assertEquals(dirs + "/F", m.get(3000L));
    assertEquals("/d1/d2/d3/d4/d5/d6/d7/d8/d9/d10/d11/d12/d13/d14/d15/d16/d17/d18/d19/d20/f20",
        m.get(2020L));
  }

  @Test
  public void cycleWithDefaultCapTerminatesWithNull() {
    Tree t = new Tree()
        .node(11, 12, "D", "x")
        .node(12, 11, "D", "y")
        .node(13, 11, "F", "in-cycle");
    PathResolver.Resolved r = stats(t, PathResolver.DEFAULT_MAX_ITERATIONS, 13, 11, 12);
    Map<Long, String> m = collect(r.paths);
    assertEquals(3, m.size());
    assertNull(m.get(13L));
    assertNull(m.get(11L));
    assertNull(m.get(12L));
    assertTrue("jumpRounds=" + r.jumpRounds, r.jumpRounds <= maxRounds());
  }

  @Test
  public void chainDeeperThanClosureCapIsNull() {
    assertNull(resolve(chain(10), 4, 1010).get(1010L));
  }

  @Test
  public void missingNameMakesPathNull() {
    Tree t = new Tree();
    t.edges.add(RowFactory.create(2L, PathResolver.ROOT));   // 2 has an edge but no inode row
    t.node(3, 2, "F", "f");
    assertNull(resolve(t, 64, 3).get(3L));
  }

  @Test
  public void duplicateParentRowsDoNotDuplicateOutput() {
    Tree t = new Tree()
        .node(2, PathResolver.ROOT, "D", "data")
        .node(3, 2, "F", "f");
    t.edges.add(RowFactory.create(3L, 2L));
    SparkSession spark = LocalSpark.session();
    List<Row> ids = new ArrayList<>();
    ids.add(RowFactory.create(3L));
    Dataset<Row> out = PathResolver.resolve(spark.createDataFrame(ids, ID),
        spark.createDataFrame(t.edges, EDGE), spark.createDataFrame(t.inodes, INODE), 64);
    List<Row> rows = out.collectAsList();
    assertEquals(1, rows.size());
    assertEquals("/data/f", rows.get(0).getString(1));
  }
}
