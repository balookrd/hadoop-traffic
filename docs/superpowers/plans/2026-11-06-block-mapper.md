# block-mapper (step 2, part B) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A command-line tool that reads a downloaded HDFS fsimage and writes the inode model (`inode`, `edge`, `block_inode`, `meta`) as ORC (or TSV) so that block ids from the agent's events can be resolved to current file paths.

**Architecture:** New Maven module `block-mapper/`. `FsImageReader` streams the needed fsimage sections (located through `FileSummary`) into an `InodeModelSink`; `OrcModelSink` / `TsvModelSink` write one file per table to any Hadoop `FileSystem` URI. `FsImageMapper` is the CLI: it writes into the hidden sibling `<parent>/.<name>._tmp` and renames it to `<out>` only on success. The tree is never built in memory: names come from `INODE`, parent links from `INODE_DIR`, and only `INODE_REFERENCE` is held in memory.

**Tech Stack:** Java 8, Hadoop 3.1.1 / 3.3.6 (`provided`; generated `FsImageProto` classes from `hadoop-hdfs`), orc-core 1.5.13 (the ORC line of Hive 3.1; `VectorizedRowBatch` writer), zlib, JUnit 4, MiniDFSCluster.

Spec: `docs/superpowers/specs/2026-11-06-block-to-file-mapping-design.md`, section 5 (and §1 layout, §8 mapper tests).
Previous plan (done): `docs/superpowers/plans/2026-11-06-agent-event-stream.md`. Next: plan C (Spark jobs, Hive DDL, stand scenario).

## Global Constraints

- Java 8 bytecode. Hadoop dependencies stay `provided`; the tool runs with `hadoop jar`.
- `block-mapper` is built per profile: `FsImageProto` references `com.google.protobuf` in 3.1 and `org.apache.hadoop.thirdparty.protobuf` in 3.3. Artifact: `block-mapper/target/block-mapper-<ver>-hadoop-<hadoop.version>.jar`. Code must never name `ByteString` (or any protobuf runtime class) explicitly — only call methods on the generated classes, so the same source compiles in both profiles.
- Format (user decision): ORC with ZLIB compression for every table of the project; here `orc-core` 1.5.13, file version `0.12`, only `bigint` and `string` columns, bloom filters on the join keys (`inode.inode_id`, `edge.child_id`, `block_inode.block_id`). Readable by Hive 3.1 on HDP and by Spark 3.5.1 (`spark.sql.orc.impl=native`); Spark 2 is not supported. `protobuf-java` is not bundled (Hadoop 3.1 and 3.3 both ship 2.5.0, which orc-core 1.5 uses); `hive-storage-api`, `orc-shims`, `aircompressor` are bundled.
- Output layout: `<out>/{inode,edge,block_inode,meta}/part-00000.<orc|tsv>`; written to the hidden sibling `<parent>/.<name>._tmp` (e.g. `fsimage/.dt=2026-11-06._tmp`, so it never looks like a partition), renamed to `<out>` on success; on failure non-zero exit, clear message, no `<out>` and no temp directory left.
- Table columns (verbatim, in this order): `inode(inode_id, type, name)` with `type` ∈ `F`/`D`/`S`; `edge(child_id, parent_id)`; `block_inode(block_id, inode_id)`; `meta(txid, image_file, parsed_at)`.
- References: an `INODE_DIR` `refChildren` entry whose reference has no name (`DstReference`, the current location) yields an edge to `referredId`; a named one (`WithName`, a snapshot view) is skipped.
- Images saved in parallel (`*_SUB` sections) are read through their parent sections; a compressed image with `*_SUB` sections is rejected with a clear error.
- Both Maven profiles must pass: `mvn clean verify -P hadoop-3.3.6` and `mvn clean verify -P hadoop-3.1.1` (`export JAVA_HOME=~/Library/Java/JavaVirtualMachines/liberica-1.8.0_432; export PATH=$JAVA_HOME/bin:$PATH`). The agent module's 114 tests must stay green.
- Repository rules: work in `main`; **never `git commit` / `git push` without an explicit command from the user** — each task ends by staging its files with `git add`; README.md and README.ru.md are updated together; code comments in English.

---

### Task 1: Module scaffold and CLI options

**Files:**
- Modify: `pom.xml` (parent: add `<module>block-mapper</module>`)
- Create: `block-mapper/pom.xml`
- Create: `block-mapper/src/main/java/org/hadooptraffic/mapper/MapperOptions.java`
- Test: `block-mapper/src/test/java/org/hadooptraffic/mapper/MapperOptionsTest.java`

**Interfaces:**
- Produces: `MapperOptions.parse(String[] args): MapperOptions` (throws `IllegalArgumentException` with a usage message); getters `image(): String`, `output(): String`, `format(): MapperOptions.Format`; enum `Format { ORC, TSV }`; constant `MapperOptions.USAGE`.

- [ ] **Step 1: Add the module to the parent**

In `pom.xml`, `<modules>`:
```xml
  <modules>
    <module>agent</module>
    <module>block-mapper</module>
  </modules>
```

- [ ] **Step 2: Create `block-mapper/pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>

  <parent>
    <groupId>org.hadooptraffic</groupId>
    <artifactId>hdfs-traffic-parent</artifactId>
    <version>0.1.0-SNAPSHOT</version>
    <relativePath>../pom.xml</relativePath>
  </parent>

  <artifactId>block-mapper</artifactId>
  <packaging>jar</packaging>

  <properties>
    <orc.version>1.5.13</orc.version>
  </properties>

  <dependencies>
    <dependency>
      <groupId>org.apache.orc</groupId>
      <artifactId>orc-core</artifactId>
      <version>${orc.version}</version>
      <exclusions>
        <!-- Hadoop 3.1 and 3.3 both ship protobuf-java 2.5.0, the version orc-core 1.5 uses. -->
        <exclusion>
          <groupId>com.google.protobuf</groupId>
          <artifactId>protobuf-java</artifactId>
        </exclusion>
        <!-- Part of the JDK on Java 8. -->
        <exclusion>
          <groupId>javax.xml.bind</groupId>
          <artifactId>jaxb-api</artifactId>
        </exclusion>
        <exclusion>
          <groupId>org.apache.hadoop</groupId>
          <artifactId>*</artifactId>
        </exclusion>
      </exclusions>
    </dependency>

    <dependency>
      <groupId>org.apache.hadoop</groupId>
      <artifactId>hadoop-common</artifactId>
      <version>${hadoop.version}</version>
      <scope>provided</scope>
    </dependency>
    <dependency>
      <groupId>org.apache.hadoop</groupId>
      <artifactId>hadoop-hdfs</artifactId>
      <version>${hadoop.version}</version>
      <scope>provided</scope>
    </dependency>
    <dependency>
      <groupId>org.apache.hadoop</groupId>
      <artifactId>hadoop-hdfs-client</artifactId>
      <version>${hadoop.version}</version>
      <scope>provided</scope>
    </dependency>

    <dependency>
      <groupId>junit</groupId>
      <artifactId>junit</artifactId>
      <version>${junit.version}</version>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.apache.hadoop</groupId>
      <artifactId>hadoop-common</artifactId>
      <version>${hadoop.version}</version>
      <type>test-jar</type>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.apache.hadoop</groupId>
      <artifactId>hadoop-hdfs</artifactId>
      <version>${hadoop.version}</version>
      <type>test-jar</type>
      <scope>test</scope>
    </dependency>
  </dependencies>

  <build>
    <finalName>block-mapper-${project.version}-hadoop-${hadoop.version}</finalName>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-compiler-plugin</artifactId>
      </plugin>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-surefire-plugin</artifactId>
      </plugin>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-shade-plugin</artifactId>
        <executions>
          <execution>
            <phase>package</phase>
            <goals>
              <goal>shade</goal>
            </goals>
            <configuration>
              <createDependencyReducedPom>false</createDependencyReducedPom>
              <artifactSet>
                <excludes>
                  <!-- Hadoop provides slf4j; a second copy in the jar would clash. -->
                  <exclude>org.slf4j:*</exclude>
                </excludes>
              </artifactSet>
              <filters>
                <filter>
                  <artifact>*:*</artifact>
                  <excludes>
                    <exclude>module-info.class</exclude>
                    <exclude>META-INF/versions/**/module-info.class</exclude>
                    <exclude>META-INF/*.SF</exclude>
                    <exclude>META-INF/*.DSA</exclude>
                    <exclude>META-INF/*.RSA</exclude>
                  </excludes>
                </filter>
              </filters>
            </configuration>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
</project>
```
orc-core brings `hive-storage-api` (the `VectorizedRowBatch` classes), `orc-shims` and `aircompressor`; they are bundled and relocated under `org.hadooptraffic.mapper.shaded` (`hadoop jar` is parent-first, Hive jars on `HADOOP_CLASSPATH` must not win). No `Main-Class` in the manifest: `hadoop jar <jar> <class> <args>` is the documented invocation (with a `Main-Class`, `RunJar` would pass the class name as the first argument).

- [ ] **Step 3: Write the failing test** — `MapperOptionsTest.java`:
```java
package org.hadooptraffic.mapper;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class MapperOptionsTest {
  @Test
  public void parsesRequiredOptionsWithOrcDefault() {
    MapperOptions o = MapperOptions.parse(
        new String[] {"-i", "/data/fsimg/fsimage_0000000000000000042", "-o", "hdfs:///x/dt=2026-11-06"});
    assertEquals("/data/fsimg/fsimage_0000000000000000042", o.image());
    assertEquals("hdfs:///x/dt=2026-11-06", o.output());
    assertEquals(MapperOptions.Format.ORC, o.format());
  }

  @Test
  public void parsesTsvFormat() {
    assertEquals(MapperOptions.Format.TSV,
        MapperOptions.parse(new String[] {"-i", "a", "-o", "b", "--format", "tsv"}).format());
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsMissingInput() {
    MapperOptions.parse(new String[] {"-o", "b"});
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsMissingOutput() {
    MapperOptions.parse(new String[] {"-i", "a"});
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsUnknownFormat() {
    MapperOptions.parse(new String[] {"-i", "a", "-o", "b", "--format", "parquet"});
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsUnknownOption() {
    MapperOptions.parse(new String[] {"-i", "a", "-o", "b", "-x"});
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsOptionWithoutValue() {
    MapperOptions.parse(new String[] {"-i", "a", "-o"});
  }
}
```

- [ ] **Step 4: Run to fail**

Run: `mvn -q -pl block-mapper test -Dtest=MapperOptionsTest`
Expected: compilation FAILURE (`MapperOptions` missing).

- [ ] **Step 5: Implement** — `MapperOptions.java`:
```java
package org.hadooptraffic.mapper;

/** Command line of {@link FsImageMapper}. */
public final class MapperOptions {
  public static final String USAGE = "usage: hadoop jar block-mapper.jar "
      + "org.hadooptraffic.mapper.FsImageMapper -i <fsimage file> -o <output dir URI> "
      + "[--format orc|tsv]";

  /** Output format of the four tables. */
  public enum Format { ORC, TSV }

  private String image;
  private String output;
  private Format format = Format.ORC;

  private MapperOptions() {
  }

  public String image() { return image; }
  public String output() { return output; }
  public Format format() { return format; }

  public static MapperOptions parse(String[] args) {
    MapperOptions o = new MapperOptions();
    for (int i = 0; i < args.length; i++) {
      String key = args[i];
      if (i + 1 >= args.length) {
        throw new IllegalArgumentException("missing value for " + key + "\n" + USAGE);
      }
      String value = args[++i];
      switch (key) {
        case "-i":
          o.image = value;
          break;
        case "-o":
          o.output = value;
          break;
        case "--format":
          if ("orc".equals(value)) {
            o.format = Format.ORC;
          } else if ("tsv".equals(value)) {
            o.format = Format.TSV;
          } else {
            throw new IllegalArgumentException("unknown format: " + value + "\n" + USAGE);
          }
          break;
        default:
          throw new IllegalArgumentException("unknown option: " + key + "\n" + USAGE);
      }
    }
    if (o.image == null || o.output == null) {
      throw new IllegalArgumentException("-i and -o are required\n" + USAGE);
    }
    return o;
  }
}
```
Note: `rejectsUnknownOption` passes `-x` as the last argument, so it fails as "missing value"; both paths throw `IllegalArgumentException`, which is what the test asserts.

- [ ] **Step 6: Run both profiles**

Run: `mvn -q clean verify -P hadoop-3.3.6` then `mvn -q clean verify -P hadoop-3.1.1`
Expected: BUILD SUCCESS; `MapperOptionsTest` 7/7; agent still 114 tests; `ls block-mapper/target/block-mapper-0.1.0-SNAPSHOT-hadoop-3.1.1.jar` exists after the second run, and `unzip -l` on it lists `org/apache/orc/OrcFile.class` and `org/apache/hadoop/hive/ql/exec/vector/VectorizedRowBatch.class`, and does not list `com/google/protobuf/`.

- [ ] **Step 7: Stage** — `git add pom.xml block-mapper`

---

### Task 2: Model sinks (ORC and TSV)

**Files:**
- Create: `block-mapper/src/main/java/org/hadooptraffic/mapper/InodeModelSink.java`
- Create: `block-mapper/src/main/java/org/hadooptraffic/mapper/Tables.java`
- Create: `block-mapper/src/main/java/org/hadooptraffic/mapper/TsvModelSink.java`
- Create: `block-mapper/src/main/java/org/hadooptraffic/mapper/OrcModelSink.java`
- Create: `block-mapper/src/test/java/org/hadooptraffic/mapper/ModelFiles.java` (test helper: reads both formats back)
- Test: `block-mapper/src/test/java/org/hadooptraffic/mapper/ModelSinksTest.java`

**Interfaces:**
- Produces:
  - `interface InodeModelSink extends Closeable { void inode(long id, char type, String name); void block(long blockId, long inodeId); void edge(long childId, long parentId); void meta(long txid, String imageFile, long parsedAtMillis); }` — all methods `throws IOException`.
  - `Tables` constants: `INODE="inode"`, `EDGE="edge"`, `BLOCK_INODE="block_inode"`, `META="meta"`; `static Path file(Path dir, String table, String ext)` → `<dir>/<table>/part-00000.<ext>`.
  - `new TsvModelSink(FileSystem fs, Path dir)`, `new OrcModelSink(Configuration conf, Path dir)`.
  - Test helper `ModelFiles.read(Configuration conf, Path dir, MapperOptions.Format f): ModelFiles` with public final fields `Map<Long, String> names`, `Map<Long, Character> types`, `Map<Long, Long> parents`, `Map<Long, Long> blockInodes`, `long txid`, `String imageFile`; and `String path(long inodeId)` (null when the chain does not reach the root inode 16385).

- [ ] **Step 1: Write the failing test** — `ModelSinksTest.java`:
```java
package org.hadooptraffic.mapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ModelSinksTest {
  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  private final Configuration conf = new Configuration();

  private static void writeSample(InodeModelSink s) throws Exception {
    s.inode(16385L, 'D', "");
    s.inode(16386L, 'D', "data");
    s.inode(16387L, 'F', "we\tird\nname\\x");
    s.inode(16388L, 'F', "orphan");
    s.block(1073741825L, 16387L);
    s.block(1073741826L, 16387L);
    s.block(-9223372036854775792L, 16388L);
    s.edge(16386L, 16385L);
    s.edge(16387L, 16386L);
    s.meta(42L, "fsimage_0000000000000000042", 1_762_430_000_000L);
    s.close();
  }

  private void roundTrip(MapperOptions.Format f, InodeModelSink sink, Path dir) throws Exception {
    writeSample(sink);
    ModelFiles m = ModelFiles.read(conf, dir, f);
    assertEquals("/data/we\tird\nname\\x", m.path(16387L));
    assertEquals("/data", m.path(16386L));
    assertEquals("/", m.path(16385L));
    assertNull(m.path(16388L));
    assertEquals(Character.valueOf('F'), m.types.get(16387L));
    assertEquals(Long.valueOf(16387L), m.blockInodes.get(1073741826L));
    assertEquals(Long.valueOf(16388L), m.blockInodes.get(-9223372036854775792L));
    assertEquals(42L, m.txid);
    assertEquals("fsimage_0000000000000000042", m.imageFile);
  }

  @Test
  public void orcRoundTrip() throws Exception {
    Path dir = new Path(tmp.getRoot().toURI().toString(), "out");
    roundTrip(MapperOptions.Format.ORC, new OrcModelSink(conf, dir), dir);
    assertTrue(new File(tmp.getRoot(), "out/inode/part-00000.orc").isFile());
    assertTrue(new File(tmp.getRoot(), "out/meta/part-00000.orc").isFile());
  }

  @Test
  public void orcFilesAreZlibCompressedVersion012() throws Exception {
    Path dir = new Path(tmp.getRoot().toURI().toString(), "out");
    writeSample(new OrcModelSink(conf, dir));
    org.apache.orc.Reader r = org.apache.orc.OrcFile.createReader(
        Tables.file(dir, Tables.BLOCK_INODE, "orc"), org.apache.orc.OrcFile.readerOptions(conf));
    assertEquals(org.apache.orc.CompressionKind.ZLIB, r.getCompressionKind());
    assertEquals(org.apache.orc.OrcFile.Version.V_0_12, r.getFileVersion());
    assertEquals("struct<block_id:bigint,inode_id:bigint>", r.getSchema().toString());
    assertEquals(3L, r.getNumberOfRows());
  }

  @Test
  public void tsvRoundTripEscapesNames() throws Exception {
    Path dir = new Path(tmp.getRoot().toURI().toString(), "out");
    FileSystem fs = dir.getFileSystem(conf);
    roundTrip(MapperOptions.Format.TSV, new TsvModelSink(fs, dir), dir);
    String inode = new String(java.nio.file.Files.readAllBytes(
        new File(tmp.getRoot(), "out/inode/part-00000.tsv").toPath()), "UTF-8");
    assertTrue(inode, inode.contains("16387\tF\twe\\tird\\nname\\\\x\n"));
  }
}
```

`ModelFiles.java` (test helper):
```java
package org.hadooptraffic.mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;

/** Reads the four tables back (either format) and resolves paths. Tests only. */
final class ModelFiles {
  static final long ROOT = 16385L;

  final Map<Long, String> names = new HashMap<>();
  final Map<Long, Character> types = new HashMap<>();
  final Map<Long, Long> parents = new HashMap<>();
  final Map<Long, Long> blockInodes = new HashMap<>();
  long txid;
  String imageFile;

  static ModelFiles read(Configuration conf, Path dir, MapperOptions.Format f) throws IOException {
    ModelFiles m = new ModelFiles();
    if (f == MapperOptions.Format.ORC) {
      for (Object[] r : orc(conf, Tables.file(dir, Tables.INODE, "orc"))) {
        long id = (Long) r[0];
        m.types.put(id, ((String) r[1]).charAt(0));
        m.names.put(id, (String) r[2]);
      }
      for (Object[] r : orc(conf, Tables.file(dir, Tables.EDGE, "orc"))) {
        m.parents.put((Long) r[0], (Long) r[1]);
      }
      for (Object[] r : orc(conf, Tables.file(dir, Tables.BLOCK_INODE, "orc"))) {
        m.blockInodes.put((Long) r[0], (Long) r[1]);
      }
      Object[] meta = orc(conf, Tables.file(dir, Tables.META, "orc")).get(0);
      m.txid = (Long) meta[0];
      m.imageFile = (String) meta[1];
    } else {
      for (String[] r : tsv(conf, Tables.file(dir, Tables.INODE, "tsv"))) {
        long id = Long.parseLong(r[0]);
        m.types.put(id, r[1].charAt(0));
        m.names.put(id, r[2]);
      }
      for (String[] r : tsv(conf, Tables.file(dir, Tables.EDGE, "tsv"))) {
        m.parents.put(Long.parseLong(r[0]), Long.parseLong(r[1]));
      }
      for (String[] r : tsv(conf, Tables.file(dir, Tables.BLOCK_INODE, "tsv"))) {
        m.blockInodes.put(Long.parseLong(r[0]), Long.parseLong(r[1]));
      }
      String[] meta = tsv(conf, Tables.file(dir, Tables.META, "tsv")).get(0);
      m.txid = Long.parseLong(meta[0]);
      m.imageFile = meta[1];
    }
    return m;
  }

  /** Absolute path of an inode, or null when its chain does not reach the root. */
  String path(long inodeId) {
    if (inodeId == ROOT) {
      return "/";
    }
    StringBuilder sb = new StringBuilder();
    long cur = inodeId;
    for (int depth = 0; depth < 10_000; depth++) {
      String name = names.get(cur);
      if (name == null) {
        return null;
      }
      sb.insert(0, "/" + name);
      Long parent = parents.get(cur);
      if (parent == null) {
        return null;
      }
      if (parent == ROOT) {
        return sb.toString();
      }
      cur = parent;
    }
    return null;
  }

  /** All rows of an ORC file; bigint columns as Long, string columns as String. */
  private static List<Object[]> orc(Configuration conf, Path p) throws IOException {
    List<Object[]> out = new ArrayList<>();
    Reader reader = OrcFile.createReader(p, OrcFile.readerOptions(conf));
    VectorizedRowBatch b = reader.getSchema().createRowBatch();
    try (RecordReader rows = reader.rows()) {
      while (rows.nextBatch(b)) {
        for (int r = 0; r < b.size; r++) {
          Object[] row = new Object[b.numCols];
          for (int c = 0; c < b.numCols; c++) {
            ColumnVector v = b.cols[c];
            int i = v.isRepeating ? 0 : r;
            if (v instanceof LongColumnVector) {
              row[c] = ((LongColumnVector) v).vector[i];
            } else {
              BytesColumnVector s = (BytesColumnVector) v;
              row[c] = new String(s.vector[i], s.start[i], s.length[i], StandardCharsets.UTF_8);
            }
          }
          out.add(row);
        }
      }
    }
    return out;
  }

  private static List<String[]> tsv(Configuration conf, Path p) throws IOException {
    List<String[]> out = new ArrayList<>();
    FileSystem fs = p.getFileSystem(conf);
    try (BufferedReader r = new BufferedReader(
        new InputStreamReader(fs.open(p), StandardCharsets.UTF_8))) {
      for (String line = r.readLine(); line != null; line = r.readLine()) {
        String[] cols = line.split("\t", -1);
        for (int i = 0; i < cols.length; i++) {
          cols[i] = TsvModelSink.unescape(cols[i]);
        }
        out.add(cols);
      }
    }
    return out;
  }
}
```

- [ ] **Step 2: Run to fail** — `mvn -q -pl block-mapper test -Dtest=ModelSinksTest` → compilation FAILURE.

- [ ] **Step 3: Implement**

`InodeModelSink.java`:
```java
package org.hadooptraffic.mapper;

import java.io.Closeable;
import java.io.IOException;

/** Receives the inode model while an fsimage is read. Not thread-safe. */
public interface InodeModelSink extends Closeable {
  /** One inode; type is 'F' (file), 'D' (directory) or 'S' (symlink); root's name is "". */
  void inode(long id, char type, String name) throws IOException;

  /** One block of a file (striped files store the block group id). */
  void block(long blockId, long inodeId) throws IOException;

  /** Current parent of an inode. */
  void edge(long childId, long parentId) throws IOException;

  /** Image metadata; written once, last. */
  void meta(long txid, String imageFile, long parsedAtMillis) throws IOException;
}
```

`Tables.java`:
```java
package org.hadooptraffic.mapper;

import org.apache.hadoop.fs.Path;

/** Table names and file layout: {@code <dir>/<table>/part-00000.<ext>}. */
public final class Tables {
  public static final String INODE = "inode";
  public static final String EDGE = "edge";
  public static final String BLOCK_INODE = "block_inode";
  public static final String META = "meta";

  private Tables() {
  }

  public static Path file(Path dir, String table, String ext) {
    return new Path(new Path(dir, table), "part-00000." + ext);
  }
}
```

`TsvModelSink.java`:
```java
package org.hadooptraffic.mapper;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Tab-separated text, for tests and manual inspection. In names, backslash, tab, newline and
 * carriage return are escaped as {@code \\}, {@code \t}, {@code \n}, {@code \r}.
 */
public final class TsvModelSink implements InodeModelSink {
  private final Writer inode;
  private final Writer edge;
  private final Writer blockInode;
  private final Writer meta;

  public TsvModelSink(FileSystem fs, Path dir) throws IOException {
    inode = open(fs, Tables.file(dir, Tables.INODE, "tsv"));
    edge = open(fs, Tables.file(dir, Tables.EDGE, "tsv"));
    blockInode = open(fs, Tables.file(dir, Tables.BLOCK_INODE, "tsv"));
    meta = open(fs, Tables.file(dir, Tables.META, "tsv"));
  }

  private static Writer open(FileSystem fs, Path p) throws IOException {
    return new BufferedWriter(new OutputStreamWriter(fs.create(p, false), StandardCharsets.UTF_8),
        1 << 16);
  }

  @Override
  public void inode(long id, char type, String name) throws IOException {
    inode.write(id + "\t" + type + "\t" + escape(name) + "\n");
  }

  @Override
  public void block(long blockId, long inodeId) throws IOException {
    blockInode.write(blockId + "\t" + inodeId + "\n");
  }

  @Override
  public void edge(long childId, long parentId) throws IOException {
    edge.write(childId + "\t" + parentId + "\n");
  }

  @Override
  public void meta(long txid, String imageFile, long parsedAtMillis) throws IOException {
    meta.write(txid + "\t" + escape(imageFile) + "\t" + parsedAtMillis + "\n");
  }

  @Override
  public void close() throws IOException {
    IOException first = null;
    for (Writer w : new Writer[] {inode, edge, blockInode, meta}) {
      try {
        w.close();
      } catch (IOException e) {
        if (first == null) {
          first = e;
        }
      }
    }
    if (first != null) {
      throw first;
    }
  }

  static String escape(String s) {
    StringBuilder sb = new StringBuilder(s.length() + 8);
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      switch (c) {
        case '\\':
          sb.append("\\\\");
          break;
        case '\t':
          sb.append("\\t");
          break;
        case '\n':
          sb.append("\\n");
          break;
        case '\r':
          sb.append("\\r");
          break;
        default:
          sb.append(c);
      }
    }
    return sb.toString();
  }

  static String unescape(String s) {
    StringBuilder sb = new StringBuilder(s.length());
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c == '\\' && i + 1 < s.length()) {
        char n = s.charAt(++i);
        sb.append(n == 't' ? '\t' : n == 'n' ? '\n' : n == 'r' ? '\r' : n);
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }
}
```

`OrcModelSink.java`:
```java
package org.hadooptraffic.mapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

/**
 * ORC output: ZLIB, file version 0.12, bigint/string columns only (readable by Hive 3.1 and
 * Spark 3.5). Rows go into one VectorizedRowBatch per table; join keys carry bloom filters.
 */
public final class OrcModelSink implements InodeModelSink {
  static final TypeDescription INODE =
      TypeDescription.fromString("struct<inode_id:bigint,type:string,name:string>");
  static final TypeDescription EDGE =
      TypeDescription.fromString("struct<child_id:bigint,parent_id:bigint>");
  static final TypeDescription BLOCK_INODE =
      TypeDescription.fromString("struct<block_id:bigint,inode_id:bigint>");
  static final TypeDescription META =
      TypeDescription.fromString("struct<txid:bigint,image_file:string,parsed_at:bigint>");

  /** One table: its writer and the batch being filled. */
  private static final class Table {
    final Writer writer;
    final VectorizedRowBatch batch;

    Table(Writer writer, TypeDescription schema) {
      this.writer = writer;
      this.batch = schema.createRowBatch();
    }

    /** Index of the next free row; writes out a full batch first. */
    int nextRow() throws IOException {
      if (batch.size == batch.getMaxSize()) {
        writer.addRowBatch(batch);
        batch.reset();
      }
      return batch.size++;
    }

    void close() throws IOException {
      if (batch.size > 0) {
        writer.addRowBatch(batch);
        batch.reset();
      }
      writer.close();
    }
  }

  private final Table inode;
  private final Table edge;
  private final Table blockInode;
  private final Table meta;

  public OrcModelSink(Configuration conf, Path dir) throws IOException {
    inode = open(conf, Tables.file(dir, Tables.INODE, "orc"), INODE, "inode_id");
    edge = open(conf, Tables.file(dir, Tables.EDGE, "orc"), EDGE, "child_id");
    blockInode = open(conf, Tables.file(dir, Tables.BLOCK_INODE, "orc"), BLOCK_INODE,
        "block_id");
    meta = open(conf, Tables.file(dir, Tables.META, "orc"), META, null);
  }

  private static Table open(Configuration conf, Path p, TypeDescription schema,
      String bloomColumn) throws IOException {
    OrcFile.WriterOptions o = OrcFile.writerOptions(conf)
        .setSchema(schema)
        .compress(CompressionKind.ZLIB)
        .version(OrcFile.Version.V_0_12);
    if (bloomColumn != null) {
      o.bloomFilterColumns(bloomColumn);
    }
    return new Table(OrcFile.createWriter(p, o), schema);
  }

  private static void setLong(VectorizedRowBatch b, int col, int row, long v) {
    ((LongColumnVector) b.cols[col]).vector[row] = v;
  }

  private static void setString(VectorizedRowBatch b, int col, int row, String v) {
    ((BytesColumnVector) b.cols[col]).setVal(row, v.getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public void inode(long id, char type, String name) throws IOException {
    int r = inode.nextRow();
    setLong(inode.batch, 0, r, id);
    setString(inode.batch, 1, r, String.valueOf(type));
    setString(inode.batch, 2, r, name);
  }

  @Override
  public void block(long blockId, long inodeId) throws IOException {
    int r = blockInode.nextRow();
    setLong(blockInode.batch, 0, r, blockId);
    setLong(blockInode.batch, 1, r, inodeId);
  }

  @Override
  public void edge(long childId, long parentId) throws IOException {
    int r = edge.nextRow();
    setLong(edge.batch, 0, r, childId);
    setLong(edge.batch, 1, r, parentId);
  }

  @Override
  public void meta(long txid, String imageFile, long parsedAtMillis) throws IOException {
    int r = meta.nextRow();
    setLong(meta.batch, 0, r, txid);
    setString(meta.batch, 1, r, imageFile);
    setLong(meta.batch, 2, r, parsedAtMillis);
  }

  @Override
  public void close() throws IOException {
    IOException first = null;
    for (Table t : Arrays.asList(inode, edge, blockInode, meta)) {
      try {
        t.close();
      } catch (IOException e) {
        if (first == null) {
          first = e;
        }
      }
    }
    if (first != null) {
      throw first;
    }
  }
}
```

- [ ] **Step 4: Run** — `mvn -q -pl block-mapper test -Dtest=ModelSinksTest` → PASS (2 tests).

- [ ] **Step 5: Stage** — `git add block-mapper`

---

### Task 3: `FsImageReader` (fsimage → sink)

**Files:**
- Create: `block-mapper/src/main/java/org/hadooptraffic/mapper/FsImageReader.java`
- Create: `block-mapper/src/test/java/org/hadooptraffic/mapper/ImageFixture.java` (test helper: builds a namespace on a MiniDFSCluster and saves an image)
- Create: `block-mapper/src/test/java/org/hadooptraffic/mapper/RecordingSink.java` (test helper)
- Test: `block-mapper/src/test/java/org/hadooptraffic/mapper/FsImageReaderIT.java`

**Interfaces:**
- Consumes: `InodeModelSink` (Task 2).
- Produces:
  - `new FsImageReader(Configuration conf)`; `void read(File image, InodeModelSink sink) throws IOException` — calls `inode`/`block` for every inode in `INODE`, `edge` for every current child in `INODE_DIR`, then `meta(txid, image.getName(), now)` last. Does not close the sink.
  - Test helper `ImageFixture.build(): ImageFixture` with fields `File image`, `Map<Long, String> livePaths` (block id → current path, from the NameNode API), `Set<Long> snapshotOnlyBlocks`, `long txid`; `close()` shuts the cluster down.
  - Test helper `RecordingSink implements InodeModelSink` with public `Map<Long,String> names`, `Map<Long,Character> types`, `Map<Long,Long> parents`, `Map<Long,Long> blockInodes`, `long txid`, `String imageFile`, `int metaCalls`, and `String path(long inodeId)` (same rule as `ModelFiles.path`).

Background for the implementer: an fsimage is `HDFSIMG1` + sections + a `FileSummary` whose length is in the last 4 bytes. `FSImageUtil.loadSummary(RandomAccessFile)` reads the summary; each `FileSummary.Section` has `name`, `offset`, `length`. Sections are independently compressed with `summary.getCodec()` (empty = none); `FSImageUtil.wrapInputStreamForCompression(conf, codec, in)` wraps a bounded stream. Section contents:
- `NS_INFO`: one delimited `FsImageProto.NameSystemSection` (`getTransactionId()`).
- `INODE`: one delimited `INodeSection` header (`getNumInodes()`), then that many delimited `INodeSection.INode`.
- `INODE_REFERENCE`: delimited `INodeReferenceSection.INodeReference` until the end; a `refChildren` value in `INODE_DIR` is the index into this list.
- `INODE_DIR`: delimited `INodeDirectorySection.DirEntry` until the end (`getParent()`, `getChildrenList()`, `getRefChildrenList()`).
In 3.3 an image saved in parallel also lists `INODE_SUB`, `INODE_DIR_SUB`, … entries that are sub-ranges of the parent sections; reading the parent sections sequentially covers them when uncompressed.

- [ ] **Step 1: Write the test helpers and the failing IT**

`RecordingSink.java`:
```java
package org.hadooptraffic.mapper;

import java.util.HashMap;
import java.util.Map;

/** Keeps everything in memory. Tests only. */
final class RecordingSink implements InodeModelSink {
  final Map<Long, String> names = new HashMap<>();
  final Map<Long, Character> types = new HashMap<>();
  final Map<Long, Long> parents = new HashMap<>();
  final Map<Long, Long> blockInodes = new HashMap<>();
  long txid = -1;
  String imageFile;
  int metaCalls;

  @Override
  public void inode(long id, char type, String name) {
    names.put(id, name);
    types.put(id, type);
  }

  @Override
  public void block(long blockId, long inodeId) {
    blockInodes.put(blockId, inodeId);
  }

  @Override
  public void edge(long childId, long parentId) {
    parents.put(childId, parentId);
  }

  @Override
  public void meta(long t, String file, long parsedAtMillis) {
    txid = t;
    imageFile = file;
    metaCalls++;
  }

  @Override
  public void close() {
  }

  String path(long inodeId) {
    if (inodeId == ModelFiles.ROOT) {
      return "/";
    }
    StringBuilder sb = new StringBuilder();
    long cur = inodeId;
    for (int depth = 0; depth < 10_000; depth++) {
      String name = names.get(cur);
      Long parent = parents.get(cur);
      if (name == null || parent == null) {
        return null;
      }
      sb.insert(0, "/" + name);
      if (parent == ModelFiles.ROOT) {
        return sb.toString();
      }
      cur = parent;
    }
    return null;
  }
}
```

`ImageFixture.java`:
```java
package org.hadooptraffic.mapper;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.namenode.FSImageTestUtil;

/**
 * A namespace exercising every mapping rule, saved to an fsimage:
 * nested dirs, multi-block files, a directory rename (Hive staging pattern), a file renamed out
 * of a snapshotted directory (DstReference), a file deleted after a snapshot (snapshot-only
 * inode) and an erasure-coded file (block group ids).
 */
final class ImageFixture implements AutoCloseable {
  static final int BLOCK = 1024 * 1024;
  static final String EC_POLICY = "XOR-2-1-1024k";

  final MiniDFSCluster cluster;
  final File image;
  final Map<Long, String> livePaths = new HashMap<>();
  final Set<Long> snapshotOnlyBlocks = new HashSet<>();
  final long txid;

  private ImageFixture(MiniDFSCluster cluster, File image, long txid) {
    this.cluster = cluster;
    this.image = image;
    this.txid = txid;
  }

  static ImageFixture build() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();
    DistributedFileSystem fs = cluster.getFileSystem();

    String[] live = {
        "/data/a/f1", "/data/a/f2", "/data/table/t1/part-0", "/data/moved-x", "/ec/e1"};
    DFSTestUtil.createFile(fs, new Path("/data/a/f1"), 2L * BLOCK + 12345, (short) 1, 1L);
    DFSTestUtil.createFile(fs, new Path("/data/a/f2"), 100, (short) 1, 2L);
    // Hive-style staging: write under a temporary directory, then rename the directory.
    DFSTestUtil.createFile(fs, new Path("/data/staging/t1/part-0"), BLOCK + 1, (short) 1, 3L);
    fs.mkdirs(new Path("/data/table"));
    fs.rename(new Path("/data/staging/t1"), new Path("/data/table/t1"));
    // Snapshot, then rename a file out of the snapshotted directory and delete another one.
    fs.mkdirs(new Path("/snap"));
    fs.allowSnapshot(new Path("/snap"));
    DFSTestUtil.createFile(fs, new Path("/snap/x"), 100, (short) 1, 4L);
    DFSTestUtil.createFile(fs, new Path("/snap/gone"), 100, (short) 1, 5L);
    fs.createSnapshot(new Path("/snap"), "s1");
    fs.rename(new Path("/snap/x"), new Path("/data/moved-x"));
    Set<Long> goneBlocks = new HashSet<>();
    for (LocatedBlock lb : fs.getClient().getLocatedBlocks("/snap/gone", 0, 100).getLocatedBlocks()) {
      goneBlocks.add(lb.getBlock().getBlockId());
    }
    fs.delete(new Path("/snap/gone"), false);
    // Erasure-coded file: the image stores block group ids.
    fs.enableErasureCodingPolicy(EC_POLICY);
    fs.mkdirs(new Path("/ec"));
    fs.setErasureCodingPolicy(new Path("/ec"), EC_POLICY);
    DFSTestUtil.createFile(fs, new Path("/ec/e1"), 3L * BLOCK, (short) 1, 6L);

    fs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_ENTER);
    fs.saveNamespace();
    fs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_LEAVE);
    File image = FSImageTestUtil.findLatestImageFile(
        cluster.getNameNode().getFSImage().getStorage().getStorageDir(0));
    long txid = cluster.getNameNode().getFSImage().getStorage().getMostRecentCheckpointTxId();

    ImageFixture f = new ImageFixture(cluster, image, txid);
    for (String p : live) {
      long len = fs.getFileStatus(new Path(p)).getLen();
      for (LocatedBlock lb : fs.getClient().getLocatedBlocks(p, 0, len).getLocatedBlocks()) {
        f.livePaths.put(lb.getBlock().getBlockId(), p);
      }
    }
    f.snapshotOnlyBlocks.addAll(goneBlocks);
    return f;
  }

  @Override
  public void close() {
    cluster.shutdown();
  }
}
```
`FsImageReaderIT.java`:
```java
package org.hadooptraffic.mapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class FsImageReaderIT {
  private static ImageFixture fixture;
  private static RecordingSink sink;

  @BeforeClass
  public static void readImage() throws Exception {
    fixture = ImageFixture.build();
    sink = new RecordingSink();
    new FsImageReader(new Configuration()).read(fixture.image, sink);
  }

  @AfterClass
  public static void stop() {
    if (fixture != null) {
      fixture.close();
    }
  }

  @Test
  public void everyLiveBlockResolvesToItsCurrentPath() {
    assertTrue(fixture.livePaths.size() >= 7);
    for (Map.Entry<Long, String> e : fixture.livePaths.entrySet()) {
      Long inode = sink.blockInodes.get(e.getKey());
      assertTrue("block " + e.getKey() + " missing", inode != null);
      assertEquals("block " + e.getKey(), e.getValue(), sink.path(inode));
    }
  }

  @Test
  public void snapshotOnlyBlocksDoNotResolve() {
    assertTrue(!fixture.snapshotOnlyBlocks.isEmpty());
    for (long b : fixture.snapshotOnlyBlocks) {
      Long inode = sink.blockInodes.get(b);
      if (inode != null) {
        assertNull(sink.path(inode));
      }
    }
  }

  @Test
  public void typesAndRoot() {
    assertEquals("", sink.names.get(ModelFiles.ROOT));
    assertEquals(Character.valueOf('D'), sink.types.get(ModelFiles.ROOT));
    long f1 = sink.blockInodes.get(fixture.livePaths.entrySet().stream()
        .filter(e -> e.getValue().equals("/data/a/f1")).findFirst().get().getKey());
    assertEquals(Character.valueOf('F'), sink.types.get(f1));
  }

  @Test
  public void metaCarriesTxidAndFileNameOnce() {
    assertEquals(1, sink.metaCalls);
    assertEquals(fixture.txid, sink.txid);
    assertEquals(fixture.image.getName(), sink.imageFile);
  }

  @Test(expected = IOException.class)
  public void rejectsAFileThatIsNotAnImage() throws Exception {
    File junk = File.createTempFile("not-an-image", ".bin");
    junk.deleteOnExit();
    Files.write(junk.toPath(), new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
    new FsImageReader(new Configuration()).read(junk, new RecordingSink());
  }
}
```
`ImageFixture` creates 3 blocks for `/data/a/f1`, 1 for `f2`, 2 for `t1/part-0`, 1 for `moved-x` and block groups for `/ec/e1` (a group of `XOR-2-1-1024k` holds 2 MiB of data with 1 MiB blocks, so 3 MiB needs 2 groups): 9 entries expected; the test asserts `>= 7` so that a different EC layout in one Hadoop line does not break it, while every entry is still checked.

- [ ] **Step 2: Run to fail** — `mvn -q -pl block-mapper verify -Dtest=FsImageReaderIT -Dsurefire.failIfNoSpecifiedTests=false` → compilation FAILURE (`FsImageReader` missing).

- [ ] **Step 3: Implement** — `FsImageReader.java`:
```java
package org.hadooptraffic.mapper;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.server.namenode.FSImageUtil;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto;
import org.apache.hadoop.util.LimitInputStream;

/**
 * Streams the sections of a protobuf fsimage that the inode model needs. Memory use is bounded
 * by the INODE_REFERENCE section (two values per reference); the tree is never built.
 *
 * <p>Calls only methods of the generated FsImageProto classes and never names a protobuf runtime
 * type, so the same source compiles against Hadoop 3.1 (com.google.protobuf) and 3.3 (shaded).
 */
public final class FsImageReader {
  private static final String SUB_SUFFIX = "_SUB";

  private final Configuration conf;

  public FsImageReader(Configuration conf) {
    this.conf = conf;
  }

  /** One INODE_REFERENCE entry: whether it is named (a snapshot view) and what it refers to. */
  private static final class Reference {
    final long referredId;
    final boolean named;

    Reference(long referredId, boolean named) {
      this.referredId = referredId;
      this.named = named;
    }
  }

  public void read(File image, InodeModelSink sink) throws IOException {
    FsImageProto.FileSummary summary;
    try (RandomAccessFile raf = new RandomAccessFile(image, "r")) {
      if (!FSImageUtil.checkFileFormat(raf)) {
        throw new IOException("not a protobuf fsimage: " + image);
      }
      summary = FSImageUtil.loadSummary(raf);
    }
    String codec = summary.getCodec();
    Map<String, FsImageProto.FileSummary.Section> sections = new HashMap<>();
    boolean parallel = false;
    for (FsImageProto.FileSummary.Section s : summary.getSectionsList()) {
      if (s.getName().endsWith(SUB_SUFFIX)) {
        parallel = true;
      } else if (!sections.containsKey(s.getName())) {
        sections.put(s.getName(), s);
      }
    }
    if (parallel && !codec.isEmpty()) {
      throw new IOException("compressed fsimage saved in parallel (" + codec + ") is not "
          + "supported; save the image with dfs.image.parallel.save=false or without "
          + "dfs.image.compress");
    }
    for (String required : new String[] {"NS_INFO", "INODE", "INODE_DIR"}) {
      if (!sections.containsKey(required)) {
        throw new IOException("fsimage has no " + required + " section: " + image);
      }
    }

    try (FileInputStream fin = new FileInputStream(image)) {
      long txid = FsImageProto.NameSystemSection
          .parseDelimitedFrom(open(fin, sections.get("NS_INFO"), codec)).getTransactionId();
      List<Reference> refs = sections.containsKey("INODE_REFERENCE")
          ? readReferences(open(fin, sections.get("INODE_REFERENCE"), codec))
          : new ArrayList<Reference>();
      readInodes(open(fin, sections.get("INODE"), codec), sink);
      readDirectories(open(fin, sections.get("INODE_DIR"), codec), refs, sink);
      sink.meta(txid, image.getName(), System.currentTimeMillis());
    }
  }

  /** A bounded, decompressed view of one section. Not closed: closing would close fin. */
  private InputStream open(FileInputStream fin, FsImageProto.FileSummary.Section s, String codec)
      throws IOException {
    fin.getChannel().position(s.getOffset());
    InputStream bounded = new BufferedInputStream(new LimitInputStream(fin, s.getLength()),
        1 << 16);
    return FSImageUtil.wrapInputStreamForCompression(conf, codec, bounded);
  }

  private static List<Reference> readReferences(InputStream in) throws IOException {
    List<Reference> refs = new ArrayList<>();
    for (FsImageProto.INodeReferenceSection.INodeReference r =
            FsImageProto.INodeReferenceSection.INodeReference.parseDelimitedFrom(in);
        r != null;
        r = FsImageProto.INodeReferenceSection.INodeReference.parseDelimitedFrom(in)) {
      refs.add(new Reference(r.getReferredId(), r.hasName()));
    }
    return refs;
  }

  private static void readInodes(InputStream in, InodeModelSink sink) throws IOException {
    FsImageProto.INodeSection header = FsImageProto.INodeSection.parseDelimitedFrom(in);
    if (header == null) {
      throw new IOException("empty INODE section");
    }
    for (long i = 0; i < header.getNumInodes(); i++) {
      FsImageProto.INodeSection.INode inode = FsImageProto.INodeSection.INode.parseDelimitedFrom(in);
      if (inode == null) {
        throw new IOException("INODE section ended after " + i + " of "
            + header.getNumInodes() + " inodes");
      }
      sink.inode(inode.getId(), typeOf(inode.getType()), inode.getName().toStringUtf8());
      if (inode.hasFile()) {
        for (HdfsProtos.BlockProto b : inode.getFile().getBlocksList()) {
          sink.block(b.getBlockId(), inode.getId());
        }
      }
    }
  }

  private static void readDirectories(InputStream in, List<Reference> refs, InodeModelSink sink)
      throws IOException {
    for (FsImageProto.INodeDirectorySection.DirEntry e =
            FsImageProto.INodeDirectorySection.DirEntry.parseDelimitedFrom(in);
        e != null;
        e = FsImageProto.INodeDirectorySection.DirEntry.parseDelimitedFrom(in)) {
      long parent = e.getParent();
      for (long child : e.getChildrenList()) {
        sink.edge(child, parent);
      }
      for (int refIndex : e.getRefChildrenList()) {
        if (refIndex < 0 || refIndex >= refs.size()) {
          throw new IOException("INODE_DIR refers to missing reference " + refIndex);
        }
        Reference r = refs.get(refIndex);
        if (!r.named) {
          // DstReference: the current location of a file renamed under snapshots.
          sink.edge(r.referredId, parent);
        }
      }
    }
  }

  private static char typeOf(FsImageProto.INodeSection.INode.Type t) {
    switch (t) {
      case FILE:
        return 'F';
      case DIRECTORY:
        return 'D';
      case SYMLINK:
        return 'S';
      default:
        throw new IllegalArgumentException("unknown inode type " + t);
    }
  }
}
```

- [ ] **Step 4: Run both profiles for the IT**

Run: `mvn -q -pl block-mapper clean verify -P hadoop-3.3.6` then `mvn -q -pl block-mapper clean verify -P hadoop-3.1.1`
Expected: `FsImageReaderIT` 5/5 in both. If `everyLiveBlockResolvesToItsCurrentPath` fails only for `/data/moved-x`, print the reference list (referredId, named) and the DirEntry of `/data` in a scratch test and report it — the DstReference rule is the suspect, do not special-case the test.

- [ ] **Step 5: Stage** — `git add block-mapper`

---

### Task 4: `FsImageMapper` CLI (temp dir, rename, exit codes)

**Files:**
- Create: `block-mapper/src/main/java/org/hadooptraffic/mapper/FsImageMapper.java`
- Test: `block-mapper/src/test/java/org/hadooptraffic/mapper/FsImageMapperIT.java`

**Interfaces:**
- Consumes: `MapperOptions` (Task 1), sinks (Task 2), `FsImageReader` (Task 3), test helpers `ImageFixture`, `ModelFiles`.
- Produces: `FsImageMapper.run(String[] args, Configuration conf, PrintStream err): int` — `0` success, `1` failure (I/O, bad image, output exists), `2` usage error; `main(String[])` calls `System.exit(run(args, new Configuration(), System.err))`.

- [ ] **Step 1: Write the failing IT** — `FsImageMapperIT.java`:
```java
package org.hadooptraffic.mapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.file.Files;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FsImageMapperIT {
  private static ImageFixture fixture;

  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  private final Configuration conf = new Configuration();
  private final ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
  private final PrintStream err = new PrintStream(errBytes, true);

  @BeforeClass
  public static void buildImage() throws Exception {
    fixture = ImageFixture.build();
  }

  @AfterClass
  public static void stop() {
    if (fixture != null) {
      fixture.close();
    }
  }

  private String out(String name) {
    return new File(tmp.getRoot(), name).toURI().toString();
  }

  private void assertResolvesLiveBlocks(String out, MapperOptions.Format f) throws Exception {
    ModelFiles m = ModelFiles.read(conf, new Path(out), f);
    for (Map.Entry<Long, String> e : fixture.livePaths.entrySet()) {
      assertEquals("block " + e.getKey(), e.getValue(), m.path(m.blockInodes.get(e.getKey())));
    }
    assertEquals(fixture.txid, m.txid);
  }

  @Test
  public void writesOrcByDefault() throws Exception {
    String out = out("dt=2026-11-06");
    assertEquals(0, FsImageMapper.run(
        new String[] {"-i", fixture.image.getPath(), "-o", out}, conf, err));
    assertResolvesLiveBlocks(out, MapperOptions.Format.ORC);
    assertFalse(new File(tmp.getRoot(), ".dt=2026-11-06._tmp").exists());
  }

  @Test
  public void writesTsvOnRequest() throws Exception {
    String out = out("tsv");
    assertEquals(0, FsImageMapper.run(
        new String[] {"-i", fixture.image.getPath(), "-o", out, "--format", "tsv"}, conf, err));
    assertResolvesLiveBlocks(out, MapperOptions.Format.TSV);
  }

  @Test
  public void badImageFailsAndLeavesNothing() throws Exception {
    File junk = tmp.newFile("junk");
    Files.write(junk.toPath(), new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
    String out = out("bad");
    assertEquals(1, FsImageMapper.run(new String[] {"-i", junk.getPath(), "-o", out}, conf, err));
    assertFalse(new File(tmp.getRoot(), "bad").exists());
    assertFalse(new File(tmp.getRoot(), ".bad._tmp").exists());
    assertTrue(errBytes.toString(), errBytes.toString().contains("block-mapper:"));
  }

  @Test
  public void existingOutputIsNotOverwritten() throws Exception {
    File existing = tmp.newFolder("exists");
    assertEquals(1, FsImageMapper.run(
        new String[] {"-i", fixture.image.getPath(), "-o", existing.toURI().toString()}, conf,
        err));
    assertEquals(0, existing.list().length);
  }

  @Test
  public void staleTempDirIsReplaced() throws Exception {
    File stale = tmp.newFolder(".again._tmp");
    Files.write(new File(stale, "junk").toPath(), new byte[] {1});
    String out = out("again");
    assertEquals(0, FsImageMapper.run(
        new String[] {"-i", fixture.image.getPath(), "-o", out}, conf, err));
    assertFalse(new File(tmp.getRoot(), "again/junk").exists());
  }

  @Test
  public void usageErrorReturnsTwo() {
    assertEquals(2, FsImageMapper.run(new String[] {"-i", "x"}, conf, err));
    assertTrue(errBytes.toString().contains("usage:"));
  }
}
```

- [ ] **Step 2: Run to fail** — `mvn -q -pl block-mapper verify -Dtest=FsImageMapperIT -Dsurefire.failIfNoSpecifiedTests=false` → compilation FAILURE.

- [ ] **Step 3: Implement** — `FsImageMapper.java`:
```java
package org.hadooptraffic.mapper;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Reads a downloaded fsimage and writes the inode model to {@code -o}. Writes into
 * {@code <parent>/.<name>._tmp} and renames it to {@code <out>} only on success.
 *
 * <pre>
 * hdfs dfsadmin -fetchImage /data/fsimg
 * hadoop jar block-mapper-&lt;ver&gt;-hadoop-&lt;x&gt;.jar org.hadooptraffic.mapper.FsImageMapper \
 *   -i /data/fsimg/fsimage_&lt;txid&gt; -o hdfs:///data/hdfs-traffic/fsimage/dt=&lt;date&gt;
 * </pre>
 */
public final class FsImageMapper {
  private FsImageMapper() {
  }

  public static void main(String[] args) {
    System.exit(run(args, new Configuration(), System.err));
  }

  /** @return 0 on success, 1 on failure, 2 on a usage error */
  public static int run(String[] args, Configuration conf, PrintStream err) {
    MapperOptions opts;
    try {
      opts = MapperOptions.parse(args);
    } catch (IllegalArgumentException e) {
      err.println("block-mapper: " + e.getMessage());
      return 2;
    }
    Path out = new Path(opts.output());
    Path tmp = new Path(out.getParent(), "." + out.getName() + "._tmp");
    FileSystem fs;
    try {
      fs = out.getFileSystem(conf);
      if (fs.exists(out)) {
        err.println("block-mapper: output already exists: " + out);
        return 1;
      }
      fs.delete(tmp, true);
    } catch (IOException e) {
      err.println("block-mapper: " + e);
      return 1;
    }
    try {
      InodeModelSink sink = opts.format() == MapperOptions.Format.ORC
          ? new OrcModelSink(conf, tmp)
          : new TsvModelSink(fs, tmp);
      try {
        new FsImageReader(conf).read(new File(opts.image()), sink);
      } finally {
        sink.close();
      }
      if (!fs.rename(tmp, out)) {
        throw new IOException("cannot rename " + tmp + " to " + out);
      }
      return 0;
    } catch (IOException | RuntimeException e) {
      err.println("block-mapper: " + e);
      try {
        fs.delete(tmp, true);
      } catch (IOException ignored) {
        // Best effort: the message above already reports the failure.
      }
      return 1;
    }
  }
}
```

- [ ] **Step 4: Run both profiles**

Run: `mvn -q clean verify -P hadoop-3.3.6` then `mvn -q clean verify -P hadoop-3.1.1`
Expected: BUILD SUCCESS; block-mapper tests (MapperOptionsTest 7, ModelSinksTest 2, FsImageReaderIT 5, FsImageMapperIT 6) green; agent 114 green.

- [ ] **Step 5: Smoke-run the shaded jar like an operator would**

After the 3.1.1 build, run the jar outside Maven against the image a test left behind is not possible (MiniDFSCluster cleans up), so instead check that the shaded jar loads its main class with only the Hadoop client jars on the classpath:
```bash
mvn -q -pl block-mapper dependency:build-classpath -Dmdep.includeScope=provided -Dmdep.outputFile=block-mapper/target/provided.cp -P hadoop-3.1.1
CP=$(cat block-mapper/target/provided.cp)
java -cp "block-mapper/target/block-mapper-0.1.0-SNAPSHOT-hadoop-3.1.1.jar:$CP" org.hadooptraffic.mapper.FsImageMapper; echo "exit=$?"
```
Expected: the usage line on stderr and `exit=2` (no `ClassNotFoundException`, no `NoClassDefFoundError` for ORC, storage-api or protobuf).

- [ ] **Step 6: Stage** — `git add block-mapper`

---

### Task 5: Documentation (README EN/RU, spec)

**Files:**
- Modify: `README.md`, `README.ru.md`
- Modify: `docs/superpowers/specs/2026-11-06-block-to-file-mapping-design.md` (§5)

- [ ] **Step 1: README — new section after "Events (JSONL)" / «События (JSONL)»**

EN heading `## Block-to-file mapping (block-mapper)`, RU heading `## Сопоставление блоков и файлов (block-mapper)`. Content (same facts in both languages):
- Purpose: turns a downloaded fsimage into the inode model used to resolve event `file_block_id` to the file's current path (resolution itself is done by the Spark jobs, a later step).
- Build: `mvn clean verify -P hadoop-3.1.1` (HDP) or `-P hadoop-3.3.6`; artifact `block-mapper/target/block-mapper-<version>-hadoop-<hadoop version>.jar`; **use the jar built for the cluster's Hadoop line** (the generated protobuf classes differ).
- Run (code block):
  ```bash
  hdfs dfsadmin -fetchImage /data/fsimg
  hadoop jar block-mapper-0.1.0-SNAPSHOT-hadoop-3.1.1.jar org.hadooptraffic.mapper.FsImageMapper \
    -i /data/fsimg/fsimage_<txid> -o hdfs:///data/hdfs-traffic/fsimage/dt=<YYYY-MM-DD> [--format tsv]
  ```
  Always on a downloaded image, never on the NameNode's own storage.
- Output: table of `inode(inode_id, type F/D/S, name)`, `edge(child_id, parent_id)`, `block_inode(block_id, inode_id)`, `meta(txid, image_file, parsed_at)`, one file each under `<out>/<table>/part-00000.orc` (ORC 0.12, ZLIB, bloom filters on the id columns; readable by Hive 3.1 and Spark 3.5). `--format tsv`: same columns, tab-separated, names escape `\\`, `\t`, `\n`, `\r`.
- Behaviour: writes to the hidden sibling `<parent>/.<name>._tmp` and renames on success; refuses an existing `<out>`; exit codes 0/1/2. Memory: only snapshot references are kept in memory, the namespace is streamed.
- Limitations: inodes visible only in snapshots (deleted after a snapshot) have no parent and stay unresolved; a compressed image saved in parallel (`dfs.image.parallel.save=true` with `dfs.image.compress=true`, Hadoop 3.3) is rejected — fetch an image saved without one of the two.

- [ ] **Step 2: Spec §5** — update to match the implementation:
  - §5.3 already describes ORC (orc-core 1.5.13, ZLIB, version 0.12, bloom filters); check it still matches the code.
  - §5.1: add "an existing `<out>` is never overwritten (exit 1); exit codes 0 / 1 / 2 (usage)".
  - §5.2: add "images saved in parallel are read through their parent sections; a compressed parallel image is rejected".
  - §8 mapper IT: note that `FsImageReaderIT` / `FsImageMapperIT` use `ImageFixture` (dir rename, snapshot + rename out, delete after snapshot, `XOR-2-1-1024k` EC file).

- [ ] **Step 3: Check** — `grep -n "block-mapper" README.md README.ru.md | head` shows the new section in both files; both READMEs mention the per-Hadoop-line jar and the parallel+compressed limitation.

- [ ] **Step 4: Stage** — `git add README.md README.ru.md docs/superpowers/specs/2026-11-06-block-to-file-mapping-design.md`
