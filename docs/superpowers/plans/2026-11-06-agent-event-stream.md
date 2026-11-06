# Agent event stream (step 2, part A) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the DataNode agent write every operation (with intermediate deltas for long ones) to rotating local JSONL files and ship them into HDFS, as stage 1 of block-to-file mapping.

**Architecture:** The project becomes a Maven multi-module build (`agent/` now; `block-mapper/` and `spark-jobs/` come in plans B and C). `OpContext` emits delta events every `flushIntervalSec`; a new `JsonlEventSink` takes events through a bounded queue and a single writer thread writes them into `RollingEventFile`, which rotates by size, age and UTC day into immutable `events-<yyyyMMdd-HHmmss>-<seq>.jsonl` files. `deploy/upload-events.sh` (cron) moves closed files into `/data/hdfs-traffic/raw/dt=…/dn=…/`.

**Tech Stack:** Java 8, ByteBuddy (existing), Hadoop 3.1.1 / 3.3.6 (`provided`), JUnit 4, Mockito 2, MiniDFSCluster, bash, Python 3 (stand checker).

Spec: `docs/superpowers/specs/2026-11-06-block-to-file-mapping-design.md` (sections 1–4, 7, 8 agent parts).
Follow-up plans: B — `block-mapper`, C — `spark-jobs`, Hive DDL and the end-to-end stand scenario.

## Global Constraints

- Java 8 bytecode; the agent stays one shaded jar for both Hadoop lines; Hadoop deps stay `provided`.
- No new runtime dependencies in the agent (no Jackson, no Kafka): JSON is formatted by hand.
- The data path never blocks, does I/O or formats strings: sinks only `offer()` to a bounded queue.
- Nothing in the sink may throw into a DataNode thread or stop the DataNode from starting.
- Unknown or invalid agent options keep disabling the agent (WARN, DataNode starts).
- Option defaults (verbatim from the spec): `eventsQueue=65536`, `eventsMaxFileMb=256`, `eventsRotateSec=300`, `eventsMaxFiles=100`, `flushIntervalSec=10` (`0` disables deltas; applies to all sinks, including JMX).
- Closed file name: `<base>-<yyyyMMdd-HHmmss>-<seq><ext>`, UTC time the file was opened; files never span a UTC day.
- Both Maven profiles must pass: `mvn clean verify -P hadoop-3.3.6` and `mvn clean verify -P hadoop-3.1.1` (JDK 8, `JAVA_HOME` set). On this machine: `export JAVA_HOME=~/Library/Java/JavaVirtualMachines/liberica-1.8.0_432; export PATH=$JAVA_HOME/bin:$PATH`. Baseline before Task 1: 56 tests, 0 failures in each profile.
- Repository rules: work directly in `main`, no new branches; **never `git commit` / `git push` without an explicit command from the user** — each task ends with a checkpoint (stage files, show the diff summary, wait).
- Docs: `README.md` and `README.ru.md` are always updated together.
- Code comments, commit messages: English.

---

### Task 1: Multi-module layout

**Files:**
- Move: `pom.xml` → `agent/pom.xml`, `src/` → `agent/src/`
- Create: `pom.xml` (parent)
- Modify: `agent/pom.xml`
- Modify: `stand/prepare.sh:44`, `stand/docker-compose.yml:31`

**Interfaces:**
- Produces: parent `org.hadooptraffic:hdfs-traffic-parent:0.1.0-SNAPSHOT` (packaging `pom`) with profiles `hadoop-3.3.6` (default) and `hadoop-3.1.1` defining `hadoop.version`; module `agent` producing `agent/target/hdfs-traffic-agent-0.1.0-SNAPSHOT.jar`. Plans B and C add `<module>` entries here.

- [ ] **Step 1: Move the sources**

```bash
cd /Users/mvmalykh/IdeaProjects/hadoop-traffic
mkdir agent
git mv pom.xml agent/pom.xml
git mv src agent/src
```

- [ ] **Step 2: Create the parent `pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>

  <groupId>org.hadooptraffic</groupId>
  <artifactId>hdfs-traffic-parent</artifactId>
  <version>0.1.0-SNAPSHOT</version>
  <packaging>pom</packaging>

  <modules>
    <module>agent</module>
  </modules>

  <properties>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    <maven.compiler.source>1.8</maven.compiler.source>
    <maven.compiler.target>1.8</maven.compiler.target>
    <hadoop.version>3.3.6</hadoop.version>
    <bytebuddy.version>1.14.19</bytebuddy.version>
    <junit.version>4.13.2</junit.version>
  </properties>

  <profiles>
    <profile>
      <id>hadoop-3.3.6</id>
      <activation>
        <activeByDefault>true</activeByDefault>
      </activation>
      <properties>
        <hadoop.version>3.3.6</hadoop.version>
      </properties>
      <dependencies>
        <dependency>
          <groupId>org.bouncycastle</groupId>
          <artifactId>bcprov-jdk15on</artifactId>
          <version>1.68</version>
          <scope>test</scope>
        </dependency>
        <dependency>
          <groupId>org.bouncycastle</groupId>
          <artifactId>bcpkix-jdk15on</artifactId>
          <version>1.68</version>
          <scope>test</scope>
        </dependency>
      </dependencies>
    </profile>
    <profile>
      <id>hadoop-3.1.1</id>
      <properties>
        <hadoop.version>3.1.1</hadoop.version>
      </properties>
      <dependencies>
        <dependency>
          <groupId>org.bouncycastle</groupId>
          <artifactId>bcprov-jdk16</artifactId>
          <version>1.46</version>
          <scope>test</scope>
        </dependency>
      </dependencies>
    </profile>
  </profiles>

  <build>
    <pluginManagement>
      <plugins>
        <plugin>
          <groupId>org.apache.maven.plugins</groupId>
          <artifactId>maven-compiler-plugin</artifactId>
          <version>3.13.0</version>
        </plugin>
        <plugin>
          <groupId>org.apache.maven.plugins</groupId>
          <artifactId>maven-surefire-plugin</artifactId>
          <version>3.2.5</version>
          <configuration>
            <forkCount>1</forkCount>
            <reuseForks>false</reuseForks>
            <argLine>-Xmx2g</argLine>
            <includes>
              <include>**/*Test.java</include>
              <include>**/*IT.java</include>
            </includes>
            <redirectTestOutputToFile>true</redirectTestOutputToFile>
          </configuration>
        </plugin>
        <plugin>
          <groupId>org.apache.maven.plugins</groupId>
          <artifactId>maven-shade-plugin</artifactId>
          <version>3.5.1</version>
        </plugin>
      </plugins>
    </pluginManagement>
  </build>
</project>
```

- [ ] **Step 3: Rewrite `agent/pom.xml` as a child**

Replace the whole file with:

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

  <artifactId>hdfs-traffic-agent</artifactId>
  <packaging>jar</packaging>

  <dependencies>
    <dependency>
      <groupId>net.bytebuddy</groupId>
      <artifactId>byte-buddy</artifactId>
      <version>${bytebuddy.version}</version>
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
      <groupId>net.bytebuddy</groupId>
      <artifactId>byte-buddy-agent</artifactId>
      <version>${bytebuddy.version}</version>
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
    <dependency>
      <groupId>org.mockito</groupId>
      <artifactId>mockito-core</artifactId>
      <version>2.28.2</version>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.apache.hadoop</groupId>
      <artifactId>hadoop-minikdc</artifactId>
      <version>${hadoop.version}</version>
      <scope>test</scope>
    </dependency>
  </dependencies>

  <build>
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
                <includes>
                  <include>net.bytebuddy:byte-buddy</include>
                </includes>
              </artifactSet>
              <relocations>
                <relocation>
                  <pattern>net.bytebuddy</pattern>
                  <shadedPattern>org.hadooptraffic.shaded.net.bytebuddy</shadedPattern>
                </relocation>
              </relocations>
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
              <transformers>
                <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                  <manifestEntries>
                    <Premain-Class>org.hadooptraffic.agent.TrafficAgent</Premain-Class>
                    <Agent-Class>org.hadooptraffic.agent.TrafficAgent</Agent-Class>
                    <Can-Retransform-Classes>true</Can-Retransform-Classes>
                    <Can-Redefine-Classes>true</Can-Redefine-Classes>
                  </manifestEntries>
                </transformer>
              </transformers>
            </configuration>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
</project>
```

- [ ] **Step 4: Point the stand at the new jar path**

`stand/prepare.sh:44`:
```bash
jar="${STAND_DIR}/../agent/target/hdfs-traffic-agent-0.1.0-SNAPSHOT.jar"
```
`stand/docker-compose.yml:31`:
```yaml
    - ${AGENT_JAR_HOST:-../agent/target/hdfs-traffic-agent-0.1.0-SNAPSHOT.jar}:/opt/agent/hdfs-traffic-agent.jar:ro
```

- [ ] **Step 5: Build both profiles**

Run: `mvn -q clean verify -P hadoop-3.3.6 && mvn -q clean verify -P hadoop-3.1.1`
Expected: BUILD SUCCESS both times, same test count as before the move; `ls agent/target/hdfs-traffic-agent-0.1.0-SNAPSHOT.jar` exists.
Also run `mvn -q help:active-profiles -pl agent -P hadoop-3.1.1 | grep hadoop-3.1.1` — expected: the profile is listed as active for the agent module.

- [ ] **Step 6: Checkpoint**

```bash
git add pom.xml agent stand/prepare.sh stand/docker-compose.yml
git status --short | head -20
```
Show the summary to the user; do not commit until told.

---

### Task 2: Event model — clock, op ids, new `TrafficEvent` fields

**Files:**
- Create: `agent/src/main/java/org/hadooptraffic/context/Clock.java`
- Create: `agent/src/main/java/org/hadooptraffic/context/OpIds.java`
- Modify: `agent/src/main/java/org/hadooptraffic/model/TrafficEvent.java`
- Create: `agent/src/test/java/org/hadooptraffic/testutil/FakeClock.java`
- Test: `agent/src/test/java/org/hadooptraffic/model/TrafficEventTest.java`, `agent/src/test/java/org/hadooptraffic/context/OpIdsTest.java`

**Interfaces:**
- Produces:
  - `interface Clock { long nanoTime(); long currentTimeMillis(); Clock SYSTEM; }`
  - `OpIds.next(): String` — `"<6 hex>-<counter>"`.
  - **Revised after the Task 3 review (user decision):** `OpIds` lives in `org.hadooptraffic.model` with `nextSeq(): long` and `format(long): String`; the `TrafficEvent` constructor takes `long opSeq` (0 = no id) in place of `String opId`, and `getOpId()` formats lazily. The code blocks below show the original version; the applied fix is in `.superpowers/sdd/es-fix-opid-brief.md`.
  - `TrafficEvent` full constructor `(String user, Op op, Role role, Locality locality, String blockPoolId, long blockId, long bytes, long estimatedBytes, long durationMs, String peer, String opId, long startMillis, long timestampMillis, boolean last, String path)`; the existing 10-arg constructor stays (opId `null`, `last=true`, `path=null`, `timestampMillis=now`, `startMillis=now-durationMs`).
  - Getters `getOpId()`, `getStartMillis()`, `getTimestampMillis()`, `isLast()`, `getPath()`, `getFileBlockId()`; static `TrafficEvent.fileBlockId(long blockId)`.
  - Test helper `org.hadooptraffic.testutil.FakeClock(long startMillis)` with `advanceMillis(long)`, `advanceSec(long)`.

- [ ] **Step 1: Write the failing tests**

`agent/src/test/java/org/hadooptraffic/model/TrafficEventTest.java`:
```java
package org.hadooptraffic.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TrafficEventTest {
  @Test
  public void fileBlockIdKeepsContiguousIds() {
    assertEquals(1073741901L, TrafficEvent.fileBlockId(1073741901L));
  }

  @Test
  public void fileBlockIdMasksStripedIndex() {
    long group = Long.MIN_VALUE + 16L * 5;
    assertEquals(group, TrafficEvent.fileBlockId(group + 3));
    assertEquals(group, TrafficEvent.fileBlockId(group));
  }

  @Test
  public void fileBlockIdKeepsNotApplicableMarker() {
    assertEquals(-1L, TrafficEvent.fileBlockId(-1L));
  }

  @Test
  public void fullConstructorKeepsAllFields() {
    TrafficEvent e = new TrafficEvent("alice", Op.READ, Role.CLIENT, Locality.REMOTE, "bp",
        Long.MIN_VALUE + 35, 10L, 2L, 300L, "peer", "abc-1", 1000L, 1300L, false, "/a");
    assertEquals("abc-1", e.getOpId());
    assertEquals(1000L, e.getStartMillis());
    assertEquals(1300L, e.getTimestampMillis());
    assertEquals(false, e.isLast());
    assertEquals("/a", e.getPath());
    assertEquals(Long.MIN_VALUE + 32, e.getFileBlockId());
  }

  @Test
  public void legacyConstructorDescribesCompleteOperation() {
    long before = System.currentTimeMillis();
    TrafficEvent e = new TrafficEvent("alice", Op.READ, Role.CLIENT, Locality.LOCAL, "bp",
        7L, 10L, 0L, 5L, "p");
    assertTrue(e.isLast());
    assertNull(e.getOpId());
    assertNull(e.getPath());
    assertTrue(e.getTimestampMillis() >= before);
    assertEquals(e.getTimestampMillis() - 5L, e.getStartMillis());
    assertEquals(7L, e.getFileBlockId());
  }
}
```

`agent/src/test/java/org/hadooptraffic/context/OpIdsTest.java`:
```java
package org.hadooptraffic.context;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class OpIdsTest {
  @Test
  public void idsAreUniqueAndShareProcessPrefix() {
    String a = OpIds.next();
    String b = OpIds.next();
    assertNotEquals(a, b);
    assertTrue(a, a.matches("[0-9a-f]{6}-\\d+"));
    assertTrue(a.substring(0, 7).equals(b.substring(0, 7)));
  }
}
```

`agent/src/test/java/org/hadooptraffic/testutil/FakeClock.java`:
```java
package org.hadooptraffic.testutil;

import org.hadooptraffic.context.Clock;

/** Manually advanced clock for deterministic tests. */
public final class FakeClock implements Clock {
  private long nanos;
  private long millis;

  public FakeClock(long startMillis) {
    this.millis = startMillis;
  }

  public synchronized void advanceMillis(long ms) {
    nanos += ms * 1_000_000L;
    millis += ms;
  }

  public void advanceSec(long sec) {
    advanceMillis(sec * 1000L);
  }

  @Override
  public synchronized long nanoTime() {
    return nanos;
  }

  @Override
  public synchronized long currentTimeMillis() {
    return millis;
  }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn -q -pl agent test -Dtest='TrafficEventTest,OpIdsTest'`
Expected: compilation FAILURE — `Clock`, `OpIds`, `fileBlockId`, new constructor not defined.

- [ ] **Step 3: Implement**

`agent/src/main/java/org/hadooptraffic/context/Clock.java`:
```java
package org.hadooptraffic.context;

/** Time source; replaceable in tests. */
public interface Clock {
  Clock SYSTEM = new Clock() {
    @Override
    public long nanoTime() {
      return System.nanoTime();
    }

    @Override
    public long currentTimeMillis() {
      return System.currentTimeMillis();
    }
  };

  long nanoTime();

  long currentTimeMillis();
}
```

`agent/src/main/java/org/hadooptraffic/context/OpIds.java`:
```java
package org.hadooptraffic.context;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/** Operation ids unique across DataNode restarts: random per-process prefix + counter. */
public final class OpIds {
  private static final String PREFIX = String.format("%06x", new Random().nextInt(1 << 24));
  private static final AtomicLong COUNTER = new AtomicLong();

  private OpIds() {
  }

  public static String next() {
    return PREFIX + "-" + COUNTER.incrementAndGet();
  }
}
```

`agent/src/main/java/org/hadooptraffic/model/TrafficEvent.java` — replace the whole file:
```java
package org.hadooptraffic.model;

/**
 * One emission of a DataNode operation: an intermediate delta ({@code last == false}) or the
 * final remainder ({@code last == true}). Immutable.
 */
public final class TrafficEvent {
  private final String user;
  private final Op op;
  private final Role role;
  private final Locality locality;
  private final String blockPoolId;
  private final long blockId;
  private final long bytes;
  private final long estimatedBytes;
  private final long durationMs;
  private final String peer;
  private final String opId;
  private final long startMillis;
  private final long timestampMillis;
  private final boolean last;
  private final String path;

  public TrafficEvent(String user, Op op, Role role, Locality locality, String blockPoolId,
      long blockId, long bytes, long estimatedBytes, long durationMs, String peer,
      String opId, long startMillis, long timestampMillis, boolean last, String path) {
    this.user = user;
    this.op = op;
    this.role = role;
    this.locality = locality;
    this.blockPoolId = blockPoolId;
    this.blockId = blockId;
    this.bytes = bytes;
    this.estimatedBytes = estimatedBytes;
    this.durationMs = durationMs;
    this.peer = peer;
    this.opId = opId;
    this.startMillis = startMillis;
    this.timestampMillis = timestampMillis;
    this.last = last;
    this.path = path;
  }

  /** A complete operation emitted now, without op id or path. */
  public TrafficEvent(String user, Op op, Role role, Locality locality, String blockPoolId,
      long blockId, long bytes, long estimatedBytes, long durationMs, String peer) {
    this(user, op, role, locality, blockPoolId, blockId, bytes, estimatedBytes, durationMs,
        peer, null, System.currentTimeMillis() - durationMs, System.currentTimeMillis(), true,
        null);
  }

  /**
   * Id of the block as the NameNode stores it: striped (negative) ids lose their index bits
   * and become the block group id; -1 (not applicable) and contiguous ids are unchanged.
   */
  public static long fileBlockId(long blockId) {
    return blockId < -1 ? blockId & ~0xFL : blockId;
  }

  public String getUser() { return user; }
  public Op getOp() { return op; }
  public Role getRole() { return role; }
  public Locality getLocality() { return locality; }
  /** Block pool id, or null for WebHDFS HTTP-leg events. */
  public String getBlockPoolId() { return blockPoolId; }
  /** Block id, or -1 when not applicable. */
  public long getBlockId() { return blockId; }
  public long getFileBlockId() { return fileBlockId(blockId); }
  /** Payload bytes since the previous event of the same operation. */
  public long getBytes() { return bytes; }
  public long getEstimatedBytes() { return estimatedBytes; }
  /** Time from operation start to this emission. */
  public long getDurationMs() { return durationMs; }
  public String getPeer() { return peer; }
  /** Groups the events of one operation; null for events built without one. */
  public String getOpId() { return opId; }
  public long getStartMillis() { return startMillis; }
  public long getTimestampMillis() { return timestampMillis; }
  /** True for the final event of an operation (counted as one op). */
  public boolean isLast() { return last; }
  /** File path when the DataNode knows it (WebHDFS HTTP leg), otherwise null. */
  public String getPath() { return path; }

  @Override
  public String toString() {
    return "TrafficEvent{user=" + user + ", op=" + op.label() + ", role=" + role.label()
        + ", locality=" + locality.label() + ", bpid=" + blockPoolId + ", blockId=" + blockId
        + ", bytes=" + bytes + ", estimatedBytes=" + estimatedBytes
        + ", durationMs=" + durationMs + ", peer=" + peer + ", opId=" + opId
        + ", last=" + last + ", path=" + path + '}';
  }
}
```

Note: `blockId < -1` (not `< 0`) keeps the `-1` marker. Real striped ids are near `Long.MIN_VALUE`, so they never collide with it.

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn -q -pl agent test -Dtest='TrafficEventTest,OpIdsTest'`
Expected: PASS (6 tests).

- [ ] **Step 5: Checkpoint**

```bash
git add agent/src
git status --short
```

---

### Task 3: Delta emission in `OpContext`; aggregator counts ops on `last`

**Files:**
- Modify: `agent/src/main/java/org/hadooptraffic/context/OpContext.java`
- Modify: `agent/src/main/java/org/hadooptraffic/aggregate/TrafficCounters.java`
- Modify: `agent/src/main/java/org/hadooptraffic/aggregate/TrafficAggregator.java`
- Test: `agent/src/test/java/org/hadooptraffic/context/OpContextTest.java`, `agent/src/test/java/org/hadooptraffic/aggregate/TrafficAggregatorTest.java`

**Interfaces:**
- Consumes: `Clock`, `OpIds`, `TrafficEvent` full constructor (Task 2), `FakeClock`.
- Produces:
  - `OpContext.setFlushIntervalSec(int sec)` (public static, `0` disables).
  - `OpContext.setClock(Clock)` (package-private static, tests only).
  - `OpContext.setPath(String)`, `OpContext.opId()`.
  - `TrafficCounters.record(long bytes, long estimated, boolean countOp)` (package-private).

- [ ] **Step 1: Write the failing tests**

`agent/src/test/java/org/hadooptraffic/context/OpContextTest.java`:
```java
package org.hadooptraffic.context;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.hadooptraffic.model.Locality;
import org.hadooptraffic.model.Op;
import org.hadooptraffic.model.Role;
import org.hadooptraffic.model.TrafficEvent;
import org.hadooptraffic.sink.TrafficSinks;
import org.hadooptraffic.testutil.FakeClock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class OpContextTest {
  private static final long T0 = 1_762_430_000_000L;
  private final List<TrafficEvent> events = new ArrayList<>();
  private final FakeClock clock = new FakeClock(T0);

  @Before
  public void setUp() {
    TrafficSinks.clear();
    TrafficSinks.register(events::add);
    OpContext.setClock(clock);
    OpContext.setFlushIntervalSec(10);
  }

  @After
  public void tearDown() {
    TrafficSinks.clear();
    OpContext.setClock(Clock.SYSTEM);
    OpContext.setFlushIntervalSec(0);
  }

  private static OpContext ctx() {
    return new OpContext("alice", Op.READ, Role.CLIENT, Locality.REMOTE, "bp", 7L, "peer");
  }

  @Test
  public void shortOperationEmitsOneFinalEvent() {
    OpContext c = ctx();
    c.addBytes(100);
    clock.advanceSec(3);
    c.addBytes(50);
    c.finish();

    assertEquals(1, events.size());
    TrafficEvent e = events.get(0);
    assertEquals(150L, e.getBytes());
    assertTrue(e.isLast());
    assertEquals(3000L, e.getDurationMs());
    assertEquals(T0, e.getStartMillis());
    assertEquals(T0 + 3000L, e.getTimestampMillis());
    assertEquals(c.opId(), e.getOpId());
  }

  @Test
  public void longOperationEmitsDeltasThenRemainder() {
    OpContext c = ctx();
    c.addBytes(100);
    clock.advanceSec(10);
    c.addBytes(20);          // 10 s elapsed: delta of 120
    clock.advanceSec(5);
    c.addBytes(30);          // 5 s since last emission: nothing
    clock.advanceSec(6);
    c.addBytes(5);           // 11 s: delta of 35
    c.finish();              // remainder 0, last

    assertEquals(3, events.size());
    assertEquals(120L, events.get(0).getBytes());
    assertFalse(events.get(0).isLast());
    assertEquals(35L, events.get(1).getBytes());
    assertFalse(events.get(1).isLast());
    assertEquals(0L, events.get(2).getBytes());
    assertTrue(events.get(2).isLast());
    long sum = 0;
    for (TrafficEvent e : events) {
      sum += e.getBytes();
      assertEquals(c.opId(), e.getOpId());
      assertEquals(T0, e.getStartMillis());
    }
    assertEquals(155L, sum);
  }

  @Test
  public void noDeltasWhenDisabled() {
    OpContext.setFlushIntervalSec(0);
    OpContext c = ctx();
    c.addBytes(1);
    clock.advanceSec(60);
    c.addBytes(1);
    c.finish();

    assertEquals(1, events.size());
    assertEquals(2L, events.get(0).getBytes());
  }

  @Test
  public void finishEmitsOnce() {
    OpContext c = ctx();
    c.finish();
    c.finish();
    assertEquals(1, events.size());
  }

  @Test
  public void estimatedBytesGoToTheFinalEvent() {
    OpContext c = ctx();
    c.addEstimatedBytes(4096);
    c.finish();
    assertEquals(4096L, events.get(0).getEstimatedBytes());
  }

  @Test
  public void pathIsCarried() {
    OpContext c = ctx();
    c.setPath("/user/alice/f");
    c.finish();
    assertEquals("/user/alice/f", events.get(0).getPath());
  }

  @Test
  public void bytesAfterFinishAreIgnored() {
    OpContext c = ctx();
    c.finish();
    clock.advanceSec(20);
    c.addBytes(10);
    assertEquals(1, events.size());
  }
}
```

Append to `agent/src/test/java/org/hadooptraffic/aggregate/TrafficAggregatorTest.java` (inside the class):
```java
  @Test
  public void deltasAddBytesButCountOneOp() {
    TrafficAggregator agg = new TrafficAggregator(10);
    agg.onEvent(new TrafficEvent("alice", Op.WRITE, Role.CLIENT, Locality.LOCAL, "bp-1", 1L,
        100L, 0L, 10_000L, "p", "x-1", 0L, 10_000L, false, null));
    agg.onEvent(new TrafficEvent("alice", Op.WRITE, Role.CLIENT, Locality.LOCAL, "bp-1", 1L,
        28L, 0L, 12_000L, "p", "x-1", 0L, 12_000L, true, null));

    TrafficCounters.Snapshot s = agg.snapshot().get(
        new TrafficKey("alice", Op.WRITE, Role.CLIENT, Locality.LOCAL));
    assertEquals(128L, s.getBytes());
    assertEquals(1L, s.getOps());
  }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn -q -pl agent test -Dtest='OpContextTest,TrafficAggregatorTest'`
Expected: compilation FAILURE (`setClock`, `setFlushIntervalSec`, `setPath`, `opId` missing).

- [ ] **Step 3: Implement**

`agent/src/main/java/org/hadooptraffic/context/OpContext.java` — replace the whole file:
```java
package org.hadooptraffic.context;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.hadooptraffic.model.Locality;
import org.hadooptraffic.model.Op;
import org.hadooptraffic.model.Role;
import org.hadooptraffic.model.TrafficEvent;
import org.hadooptraffic.sink.TrafficSinks;

/**
 * Mutable state of one in-flight operation. Emits an intermediate event with the bytes
 * accumulated since the previous emission once per flush interval, and the remainder with
 * {@code last = true} on {@link #finish()}.
 */
public final class OpContext {
  private static volatile Clock clock = Clock.SYSTEM;
  private static volatile long flushIntervalNanos;

  private final String user;
  private final Op op;
  private final Role role;
  private final Locality locality;
  private final String blockPoolId;
  private final long blockId;
  private final String peer;
  private final String opId = OpIds.next();
  private final long startNanos;
  private final long startMillis;
  // Atomics: WebHDFS contexts may be touched from more than one thread.
  private final AtomicLong bytes = new AtomicLong();
  private final AtomicLong estimatedBytes = new AtomicLong();
  private final AtomicBoolean finished = new AtomicBoolean();
  private volatile String path;
  // Written under the lock; read without it as a cheap "not due yet" check.
  private volatile long lastEmitNanos;
  // Guarded by this.
  private long emittedBytes;
  private long emittedEstimatedBytes;

  public OpContext(String user, Op op, Role role, Locality locality, String blockPoolId,
      long blockId, String peer) {
    this.user = user;
    this.op = op;
    this.role = role;
    this.locality = locality;
    this.blockPoolId = blockPoolId;
    this.blockId = blockId;
    this.peer = peer;
    Clock c = clock;
    this.startNanos = c.nanoTime();
    this.startMillis = c.currentTimeMillis();
    this.lastEmitNanos = startNanos;
  }

  /** Interval of intermediate events for all operations; 0 disables them. */
  public static void setFlushIntervalSec(int sec) {
    flushIntervalNanos = TimeUnit.SECONDS.toNanos(Math.max(0, sec));
  }

  /** For tests. */
  static void setClock(Clock c) {
    clock = c;
  }

  public String opId() {
    return opId;
  }

  public void setPath(String p) {
    path = p;
  }

  public void addBytes(long n) {
    if (n > 0) {
      bytes.addAndGet(n);
      maybeEmitDelta();
    }
  }

  public void addEstimatedBytes(long n) {
    if (n > 0) {
      estimatedBytes.addAndGet(n);
    }
  }

  public long bytes() {
    return bytes.get();
  }

  private void maybeEmitDelta() {
    long interval = flushIntervalNanos;
    if (interval <= 0) {
      return;
    }
    long now = clock.nanoTime();
    if (now - lastEmitNanos < interval) {
      return;
    }
    synchronized (this) {
      if (finished.get() || now - lastEmitNanos < interval) {
        return;
      }
      emit(now, false);
    }
  }

  /** Emits the remainder as the final event; only the first call has an effect. */
  public void finish() {
    if (!finished.compareAndSet(false, true)) {
      return;
    }
    synchronized (this) {
      emit(clock.nanoTime(), true);
    }
  }

  /** Caller holds the lock. Emitting under the lock keeps one operation's events ordered. */
  private void emit(long nowNanos, boolean last) {
    long b = bytes.get();
    long est = estimatedBytes.get();
    long deltaBytes = b - emittedBytes;
    long deltaEstimated = est - emittedEstimatedBytes;
    lastEmitNanos = nowNanos;
    if (!last && deltaBytes == 0 && deltaEstimated == 0) {
      return;
    }
    emittedBytes = b;
    emittedEstimatedBytes = est;
    TrafficSinks.emit(new TrafficEvent(user, op, role, locality, blockPoolId, blockId,
        deltaBytes, deltaEstimated, (nowNanos - startNanos) / 1_000_000L, peer, opId,
        startMillis, clock.currentTimeMillis(), last, path));
  }
}
```

`TrafficCounters.record` — replace the method:
```java
  void record(long b, long estimated, boolean countOp) {
    if (b > 0) {
      bytes.add(b);
    }
    if (estimated > 0) {
      estimatedBytes.add(estimated);
    }
    if (countOp) {
      ops.increment();
    }
  }
```

`TrafficAggregator.onEvent` — change the last statement to:
```java
    counters.computeIfAbsent(key, k -> new TrafficCounters())
        .record(e.getBytes(), e.getEstimatedBytes(), e.isLast());
```

- [ ] **Step 4: Run the whole unit suite**

Run: `mvn -q -pl agent test -Dtest='*Test'`
Expected: PASS, including the existing `XceiverHooksTest`, `WebHdfsHooksTest` (delta interval is 0 unless a test sets it).

- [ ] **Step 5: Checkpoint**

```bash
git add agent/src
git status --short
```

---

### Task 4: New agent options

**Files:**
- Modify: `agent/src/main/java/org/hadooptraffic/agent/AgentConfig.java`
- Test: `agent/src/test/java/org/hadooptraffic/agent/AgentConfigTest.java`

**Interfaces:**
- Produces: `AgentConfig` getters `maxUsers()`, `eventsPath()` (String, null when off), `eventsQueue()`, `eventsMaxFileMb()`, `eventsRotateSec()`, `eventsMaxFiles()`, `flushIntervalSec()` (all `int` except `eventsPath`); constants `DEFAULT_EVENTS_QUEUE=65536`, `DEFAULT_EVENTS_MAX_FILE_MB=256`, `DEFAULT_EVENTS_ROTATE_SEC=300`, `DEFAULT_EVENTS_MAX_FILES=100`, `DEFAULT_FLUSH_INTERVAL_SEC=10`.

- [ ] **Step 1: Write the failing tests** — append to `AgentConfigTest`:
```java
  @Test
  public void eventDefaults() {
    AgentConfig c = AgentConfig.parse(null);
    assertNull(c.eventsPath());
    assertEquals(65536, c.eventsQueue());
    assertEquals(256, c.eventsMaxFileMb());
    assertEquals(300, c.eventsRotateSec());
    assertEquals(100, c.eventsMaxFiles());
    assertEquals(10, c.flushIntervalSec());
  }

  @Test
  public void parsesEventOptions() {
    AgentConfig c = AgentConfig.parse("maxUsers=5,events=/var/log/t/events.jsonl,eventsQueue=10,"
        + "eventsMaxFileMb=1,eventsRotateSec=30,eventsMaxFiles=3,flushIntervalSec=0");
    assertEquals(5, c.maxUsers());
    assertEquals("/var/log/t/events.jsonl", c.eventsPath());
    assertEquals(10, c.eventsQueue());
    assertEquals(1, c.eventsMaxFileMb());
    assertEquals(30, c.eventsRotateSec());
    assertEquals(3, c.eventsMaxFiles());
    assertEquals(0, c.flushIntervalSec());
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsEmptyEventsPath() {
    AgentConfig.parse("events=");
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsNonPositiveQueue() {
    AgentConfig.parse("eventsQueue=0");
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsNegativeFlushInterval() {
    AgentConfig.parse("flushIntervalSec=-1");
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsNonNumericValue() {
    AgentConfig.parse("eventsMaxFiles=many");
  }
```
Add `import static org.junit.Assert.assertNull;`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn -q -pl agent test -Dtest=AgentConfigTest`
Expected: compilation FAILURE (getters missing).

- [ ] **Step 3: Implement** — replace `AgentConfig.java`:
```java
package org.hadooptraffic.agent;

/** Parsed -javaagent option string: comma-separated key=value pairs. */
public final class AgentConfig {
  public static final int DEFAULT_MAX_USERS = 2000;
  public static final int DEFAULT_EVENTS_QUEUE = 65536;
  public static final int DEFAULT_EVENTS_MAX_FILE_MB = 256;
  public static final int DEFAULT_EVENTS_ROTATE_SEC = 300;
  public static final int DEFAULT_EVENTS_MAX_FILES = 100;
  public static final int DEFAULT_FLUSH_INTERVAL_SEC = 10;

  private int maxUsers = DEFAULT_MAX_USERS;
  private String eventsPath;
  private int eventsQueue = DEFAULT_EVENTS_QUEUE;
  private int eventsMaxFileMb = DEFAULT_EVENTS_MAX_FILE_MB;
  private int eventsRotateSec = DEFAULT_EVENTS_ROTATE_SEC;
  private int eventsMaxFiles = DEFAULT_EVENTS_MAX_FILES;
  private int flushIntervalSec = DEFAULT_FLUSH_INTERVAL_SEC;

  private AgentConfig() {
  }

  public int maxUsers() { return maxUsers; }
  /** Active events file, or null when the JSONL sink is off. */
  public String eventsPath() { return eventsPath; }
  public int eventsQueue() { return eventsQueue; }
  public int eventsMaxFileMb() { return eventsMaxFileMb; }
  public int eventsRotateSec() { return eventsRotateSec; }
  public int eventsMaxFiles() { return eventsMaxFiles; }
  public int flushIntervalSec() { return flushIntervalSec; }

  public static AgentConfig parse(String args) {
    AgentConfig c = new AgentConfig();
    if (args == null) {
      return c;
    }
    for (String part : args.split(",")) {
      String p = part.trim();
      if (p.isEmpty()) {
        continue;
      }
      int eq = p.indexOf('=');
      if (eq < 0) {
        throw new IllegalArgumentException("Bad agent option (expected key=value): " + p);
      }
      String key = p.substring(0, eq).trim();
      String value = p.substring(eq + 1).trim();
      switch (key) {
        case "maxUsers":
          c.maxUsers = atLeast(key, value, 1);
          break;
        case "events":
          if (value.isEmpty()) {
            throw new IllegalArgumentException("events must be a file path");
          }
          c.eventsPath = value;
          break;
        case "eventsQueue":
          c.eventsQueue = atLeast(key, value, 1);
          break;
        case "eventsMaxFileMb":
          c.eventsMaxFileMb = atLeast(key, value, 1);
          break;
        case "eventsRotateSec":
          c.eventsRotateSec = atLeast(key, value, 1);
          break;
        case "eventsMaxFiles":
          c.eventsMaxFiles = atLeast(key, value, 1);
          break;
        case "flushIntervalSec":
          c.flushIntervalSec = atLeast(key, value, 0);
          break;
        default:
          throw new IllegalArgumentException("Unknown agent option: " + key);
      }
    }
    return c;
  }

  private static int atLeast(String key, String value, int min) {
    int v;
    try {
      v = Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(key + " must be an integer: " + value);
    }
    if (v < min) {
      throw new IllegalArgumentException(key + " must be >= " + min + ": " + v);
    }
    return v;
  }
}
```

- [ ] **Step 4: Run tests** — `mvn -q -pl agent test -Dtest=AgentConfigTest` → PASS (11 tests).

- [ ] **Step 5: Checkpoint** — `git add agent/src && git status --short`.

---

### Task 5: JSON line formatter

**Files:**
- Create: `agent/src/main/java/org/hadooptraffic/sink/EventJson.java`
- Test: `agent/src/test/java/org/hadooptraffic/sink/EventJsonTest.java`

**Interfaces:**
- Consumes: `TrafficEvent` getters (Task 2).
- Produces: `EventJson.format(TrafficEvent e, String dn): String` — one line, no trailing newline, field order exactly as in spec section 3.3.

- [ ] **Step 1: Write the failing test**
```java
package org.hadooptraffic.sink;

import static org.junit.Assert.assertEquals;

import org.hadooptraffic.model.Locality;
import org.hadooptraffic.model.Op;
import org.hadooptraffic.model.OpIds;
import org.hadooptraffic.model.Role;
import org.hadooptraffic.model.TrafficEvent;
import org.junit.Test;

public class EventJsonTest {
  @Test
  public void formatsAllFieldsInSpecOrder() {
    TrafficEvent e = new TrafficEvent("alice", Op.READ, Role.CLIENT, Locality.REMOTE, "BP-1",
        1073741901L, 64L, 0L, 10123L, "/10.0.0.5:51234", 7L, 1762429990000L,
        1762430000123L, false, null);
    assertEquals("{\"ts\":1762430000123,\"start\":1762429990000,\"dn\":\"dn1\","
        + "\"op_id\":\"" + OpIds.format(7L) + "\",\"user\":\"alice\",\"op\":\"read\",\"role\":\"client\","
        + "\"locality\":\"remote\",\"bpid\":\"BP-1\",\"block_id\":1073741901,"
        + "\"file_block_id\":1073741901,\"bytes\":64,\"est_bytes\":0,\"dur_ms\":10123,"
        + "\"last\":false,\"peer\":\"/10.0.0.5:51234\",\"path\":null}",
        EventJson.format(e, "dn1"));
  }

  @Test
  public void escapesStrings() {
    TrafficEvent e = new TrafficEvent("we\"ird\\u\tser\n", Op.WEBHDFS_CREATE, Role.CLIENT,
        Locality.LOCAL, null, -1L, 1L, 0L, 0L, null, 1L, 0L, 0L, true,
        "/a b/\u0001ü");
    String json = EventJson.format(e, "dn1");
    assertEquals(true, json.contains("\"user\":\"we\\\"ird\\\\u\\tser\\n\""));
    assertEquals(true, json.contains("\"path\":\"/a b/\\u0001ü\""));
    assertEquals(true, json.contains("\"bpid\":null"));
    assertEquals(true, json.contains("\"block_id\":-1,\"file_block_id\":-1"));
  }
}
```

- [ ] **Step 2: Run to fail** — `mvn -q -pl agent test -Dtest=EventJsonTest` → compilation FAILURE.

- [ ] **Step 3: Implement**
```java
package org.hadooptraffic.sink;

import org.hadooptraffic.model.TrafficEvent;

/** Formats one event as a single JSON line (no trailing newline). No JSON library needed. */
public final class EventJson {
  private EventJson() {
  }

  public static String format(TrafficEvent e, String dn) {
    StringBuilder sb = new StringBuilder(384);
    sb.append("{\"ts\":").append(e.getTimestampMillis());
    num(sb, "start", e.getStartMillis());
    str(sb, "dn", dn);
    str(sb, "op_id", e.getOpId());
    str(sb, "user", e.getUser());
    str(sb, "op", e.getOp().label());
    str(sb, "role", e.getRole().label());
    str(sb, "locality", e.getLocality().label());
    str(sb, "bpid", e.getBlockPoolId());
    num(sb, "block_id", e.getBlockId());
    num(sb, "file_block_id", e.getFileBlockId());
    num(sb, "bytes", e.getBytes());
    num(sb, "est_bytes", e.getEstimatedBytes());
    num(sb, "dur_ms", e.getDurationMs());
    sb.append(",\"last\":").append(e.isLast());
    str(sb, "peer", e.getPeer());
    str(sb, "path", e.getPath());
    return sb.append('}').toString();
  }

  private static void num(StringBuilder sb, String key, long v) {
    sb.append(",\"").append(key).append("\":").append(v);
  }

  private static void str(StringBuilder sb, String key, String v) {
    sb.append(",\"").append(key).append("\":");
    if (v == null) {
      sb.append("null");
      return;
    }
    sb.append('"');
    for (int i = 0; i < v.length(); i++) {
      char c = v.charAt(i);
      switch (c) {
        case '"':
          sb.append("\\\"");
          break;
        case '\\':
          sb.append("\\\\");
          break;
        case '\n':
          sb.append("\\n");
          break;
        case '\r':
          sb.append("\\r");
          break;
        case '\t':
          sb.append("\\t");
          break;
        default:
          if (c < 0x20) {
            sb.append(String.format("\\u%04x", (int) c));
          } else {
            sb.append(c);
          }
      }
    }
    sb.append('"');
  }
}
```

- [ ] **Step 4: Run** — `mvn -q -pl agent test -Dtest=EventJsonTest` → PASS.

- [ ] **Step 5: Checkpoint** — `git add agent/src && git status --short`.

---

### Task 6: `RollingEventFile` (active file, rotation, retention)

**Files:**
- Create: `agent/src/main/java/org/hadooptraffic/sink/RollingEventFile.java`
- Test: `agent/src/test/java/org/hadooptraffic/sink/RollingEventFileTest.java`

**Interfaces:**
- Consumes: `Clock`, `FakeClock`.
- Produces (package-private, single-threaded use): `RollingEventFile(Path active, long maxBytes, long maxAgeMillis, int maxFiles, Clock clock)`; `void write(String line)`, `void flush()`, `void maybeRotate()`, `void rotate()`, `void abandon()`, `void close()`, `long filesDropped()`, `List<Path> closedFiles()` — all I/O methods throw `IOException`.

- [ ] **Step 1: Write the failing tests**
```java
package org.hadooptraffic.sink;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.hadooptraffic.testutil.FakeClock;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class RollingEventFileTest {
  // 2026-11-06T10:15:00Z
  private static final long T0 = 1_793_960_100_000L;

  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  private final FakeClock clock = new FakeClock(T0);

  private RollingEventFile file(Path active, long maxBytes, long maxAgeMs, int maxFiles) {
    return new RollingEventFile(active, maxBytes, maxAgeMs, maxFiles, clock);
  }

  @Test
  public void writesLinesToActiveFileAndCreatesDirectory() throws Exception {
    Path active = tmp.getRoot().toPath().resolve("sub/dir/events.jsonl");
    RollingEventFile f = file(active, 1 << 20, 300_000, 10);
    f.write("{\"a\":1}");
    f.write("{\"a\":2}");
    f.flush();
    assertEquals(Arrays.asList("{\"a\":1}", "{\"a\":2}"), Files.readAllLines(active, UTF_8));
  }

  @Test
  public void closedFileIsNamedAfterUtcOpenTime() throws Exception {
    Path active = tmp.getRoot().toPath().resolve("events.jsonl");
    RollingEventFile f = file(active, 1 << 20, 300_000, 10);
    f.write("x");
    clock.advanceSec(42);
    f.rotate();
    assertFalse(Files.exists(active));
    List<Path> closed = f.closedFiles();
    assertEquals(1, closed.size());
    assertEquals("events-20261106-101500-000000.jsonl", closed.get(0).getFileName().toString());
    assertEquals(Collections.singletonList("x"), Files.readAllLines(closed.get(0), UTF_8));
  }

  @Test
  public void rotatesBySize() throws Exception {
    Path active = tmp.getRoot().toPath().resolve("events.jsonl");
    RollingEventFile f = file(active, 10, 300_000, 10);
    f.write("12345");
    f.maybeRotate();
    assertTrue(Files.exists(active));
    f.write("67890");
    f.maybeRotate();
    assertFalse(Files.exists(active));
    assertEquals(1, f.closedFiles().size());
  }

  @Test
  public void rotatesByAge() throws Exception {
    Path active = tmp.getRoot().toPath().resolve("events.jsonl");
    RollingEventFile f = file(active, 1 << 20, 300_000, 10);
    f.write("x");
    clock.advanceSec(299);
    f.maybeRotate();
    assertTrue(Files.exists(active));
    clock.advanceSec(1);
    f.maybeRotate();
    assertFalse(Files.exists(active));
  }

  @Test
  public void neverSpansUtcMidnight() throws Exception {
    FakeClock late = new FakeClock(1_793_923_199_000L); // 2026-11-05T23:59:59Z
    Path active = tmp.getRoot().toPath().resolve("events.jsonl");
    RollingEventFile f = new RollingEventFile(active, 1 << 20, 300_000, 10, late);
    f.write("x");
    late.advanceSec(2);
    f.maybeRotate();
    assertEquals("events-20261105-235959-000000.jsonl",
        f.closedFiles().get(0).getFileName().toString());
  }

  @Test
  public void keepsAtMostMaxClosedFiles() throws Exception {
    Path active = tmp.getRoot().toPath().resolve("events.jsonl");
    RollingEventFile f = file(active, 1 << 20, 300_000, 2);
    for (int i = 0; i < 3; i++) {
      f.write("line" + i);
      f.rotate();
      clock.advanceSec(1);
    }
    List<Path> closed = f.closedFiles();
    assertEquals(2, closed.size());
    assertEquals(Collections.singletonList("line1"), Files.readAllLines(closed.get(0), UTF_8));
    assertEquals(1L, f.filesDropped());
  }

  @Test
  public void leftoverActiveFileIsRotatedOnOpen() throws Exception {
    Path active = tmp.getRoot().toPath().resolve("events.jsonl");
    Files.write(active, "old\n".getBytes(UTF_8));
    RollingEventFile f = file(active, 1 << 20, 300_000, 10);
    f.write("new");
    f.flush();
    assertEquals(Collections.singletonList("new"), Files.readAllLines(active, UTF_8));
    List<Path> closed = f.closedFiles();
    assertEquals(1, closed.size());
    assertEquals(Collections.singletonList("old"), Files.readAllLines(closed.get(0), UTF_8));
  }

  @Test
  public void closeRotatesActiveFile() throws Exception {
    Path active = tmp.getRoot().toPath().resolve("events.jsonl");
    RollingEventFile f = file(active, 1 << 20, 300_000, 10);
    f.write("x");
    f.close();
    assertFalse(Files.exists(active));
    assertEquals(1, f.closedFiles().size());
  }

  @Test
  public void sameSecondRotationsGetDistinctNames() throws Exception {
    Path active = tmp.getRoot().toPath().resolve("events.jsonl");
    RollingEventFile f = file(active, 1 << 20, 300_000, 10);
    f.write("a");
    f.rotate();
    f.write("b");
    f.rotate();
    assertEquals(2, f.closedFiles().size());
  }
}
```

- [ ] **Step 2: Run to fail** — `mvn -q -pl agent test -Dtest=RollingEventFileTest` → compilation FAILURE.

- [ ] **Step 3: Implement**
```java
package org.hadooptraffic.sink;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.Closeable;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.hadooptraffic.context.Clock;

/**
 * The active events file plus its closed, immutable siblings
 * {@code <base>-<yyyyMMdd-HHmmss>-<seq><ext>}, named after the UTC time the file was opened.
 * A file never spans a UTC day, so the date in the name is the date of every event in it.
 * Not thread-safe: used only by the sink's writer thread.
 */
final class RollingEventFile implements Closeable {
  private static final DateTimeFormatter STAMP =
      DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").withZone(ZoneOffset.UTC);
  private static final long DAY_MS = 86_400_000L;

  private final Path active;
  private final Path dir;
  private final String base;
  private final String ext;
  private final long maxBytes;
  private final long maxAgeMillis;
  private final int maxFiles;
  private final Clock clock;
  private final AtomicLong filesDropped = new AtomicLong();

  private Writer out;
  private long size;
  private long openedMillis;
  private int seq;

  RollingEventFile(Path active, long maxBytes, long maxAgeMillis, int maxFiles, Clock clock) {
    this.active = active.toAbsolutePath();
    this.dir = this.active.getParent();
    String name = this.active.getFileName().toString();
    int dot = name.lastIndexOf('.');
    this.base = dot > 0 ? name.substring(0, dot) : name;
    this.ext = dot > 0 ? name.substring(dot) : "";
    this.maxBytes = maxBytes;
    this.maxAgeMillis = maxAgeMillis;
    this.maxFiles = maxFiles;
    this.clock = clock;
  }

  /** Appends one line, opening the active file first if needed. */
  void write(String line) throws IOException {
    if (out == null) {
      open();
    }
    out.write(line);
    out.write('\n');
    size += line.length() + 1; // approximate: JSON is ASCII except user names and paths
  }

  void flush() throws IOException {
    if (out != null) {
      out.flush();
    }
  }

  /** Rotates when the active file is too big, too old or was opened on another UTC day. */
  void maybeRotate() throws IOException {
    if (out == null) {
      return;
    }
    long now = clock.currentTimeMillis();
    if (size >= maxBytes || now - openedMillis >= maxAgeMillis
        || now / DAY_MS != openedMillis / DAY_MS) {
      rotate();
    }
  }

  /** Closes the active file and renames it to its closed name; drops the oldest extras. */
  void rotate() throws IOException {
    if (out != null) {
      out.close();
      out = null;
    }
    if (Files.exists(active) && Files.size(active) > 0) {
      Files.move(active, closedName(openedMillis));
    }
    size = 0;
    cleanup();
  }

  /** After an I/O error: forget the writer; the next write reopens (and rotates leftovers). */
  void abandon() {
    if (out != null) {
      try {
        out.close();
      } catch (IOException ignored) {
        // The file may be gone or the disk full; nothing more to do.
      }
      out = null;
    }
  }

  @Override
  public void close() throws IOException {
    rotate();
  }

  long filesDropped() {
    return filesDropped.get();
  }

  /** Closed files, oldest first. */
  List<Path> closedFiles() throws IOException {
    List<Path> files = new ArrayList<>();
    if (!Files.isDirectory(dir)) {
      return files;
    }
    try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir, base + "-*" + ext)) {
      for (Path p : ds) {
        files.add(p);
      }
    }
    Collections.sort(files);
    return files;
  }

  private void open() throws IOException {
    Files.createDirectories(dir);
    if (Files.exists(active) && Files.size(active) > 0) {
      // Left over from a crash or an earlier I/O error: close it out under its own date.
      openedMillis = Files.getLastModifiedTime(active).toMillis();
      Files.move(active, closedName(openedMillis));
      cleanup();
    }
    out = Files.newBufferedWriter(active, UTF_8, StandardOpenOption.CREATE,
        StandardOpenOption.APPEND);
    openedMillis = clock.currentTimeMillis();
    size = 0;
  }

  private Path closedName(long millis) {
    String stamp = STAMP.format(Instant.ofEpochMilli(millis));
    while (true) {
      Path p = dir.resolve(base + "-" + stamp + "-" + String.format("%06d", seq++) + ext);
      if (!Files.exists(p)) {
        return p;
      }
    }
  }

  private void cleanup() throws IOException {
    List<Path> closed = closedFiles();
    for (int i = 0; i < closed.size() - maxFiles; i++) {
      Files.deleteIfExists(closed.get(i));
      filesDropped.incrementAndGet();
    }
  }
}
```

- [ ] **Step 4: Run** — `mvn -q -pl agent test -Dtest=RollingEventFileTest` → PASS (9 tests).

- [ ] **Step 5: Checkpoint** — `git add agent/src && git status --short`.

---

### Task 7: `JsonlEventSink` (queue, writer thread, counters)

**Files:**
- Create: `agent/src/main/java/org/hadooptraffic/sink/EventSinkStats.java`
- Create: `agent/src/main/java/org/hadooptraffic/sink/JsonlEventSink.java`
- Test: `agent/src/test/java/org/hadooptraffic/sink/JsonlEventSinkTest.java`

**Interfaces:**
- Consumes: `EventJson.format`, `RollingEventFile`, `Clock`, `AgentLog.warn(String, Throwable)`.
- Produces:
  - `interface EventSinkStats { long eventsWritten(); long eventsDropped(); long queueSize(); long filesDropped(); }`
  - `JsonlEventSink(Path activeFile, int queueCapacity, long maxFileBytes, long rotateMillis, int maxFiles, String dn, Clock clock)`, `start()`, `onEvent(TrafficEvent)`, `boolean awaitIdle(long timeoutMs)`, `close(long timeoutMs)`, `Path activeFile()`.

- [ ] **Step 1: Write the failing tests**
```java
package org.hadooptraffic.sink;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.hadooptraffic.model.Locality;
import org.hadooptraffic.model.Op;
import org.hadooptraffic.model.Role;
import org.hadooptraffic.model.TrafficEvent;
import org.hadooptraffic.testutil.FakeClock;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class JsonlEventSinkTest {
  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  private final FakeClock clock = new FakeClock(1_793_960_100_000L);
  private JsonlEventSink sink;

  @After
  public void tearDown() {
    if (sink != null) {
      sink.close(2000);
    }
  }

  private JsonlEventSink sink(Path active, int capacity) {
    sink = new JsonlEventSink(active, capacity, 1 << 20, 300_000, 10, "dn1", clock);
    return sink;
  }

  private static TrafficEvent ev(long bytes) {
    return new TrafficEvent("alice", Op.READ, Role.CLIENT, Locality.LOCAL, "bp", 1L, bytes, 0L,
        1L, "p");
  }

  @Test
  public void writesAcceptedEvents() throws Exception {
    Path active = tmp.getRoot().toPath().resolve("events.jsonl");
    JsonlEventSink s = sink(active, 100);
    s.start();
    s.onEvent(ev(1));
    s.onEvent(ev(2));
    s.onEvent(ev(3));
    assertTrue(s.awaitIdle(5000));

    List<String> lines = Files.readAllLines(active, UTF_8);
    assertEquals(3, lines.size());
    assertTrue(lines.get(0).startsWith("{\"ts\":"));
    assertTrue(lines.get(0).contains("\"dn\":\"dn1\""));
    assertEquals(3L, s.eventsWritten());
    assertEquals(0L, s.eventsDropped());
  }

  @Test
  public void dropsWhenQueueIsFull() throws Exception {
    Path active = tmp.getRoot().toPath().resolve("events.jsonl");
    JsonlEventSink s = sink(active, 2);
    s.onEvent(ev(1));
    s.onEvent(ev(2));
    s.onEvent(ev(3));          // writer not started: queue is full
    assertEquals(1L, s.eventsDropped());
    assertEquals(2L, s.queueSize());

    s.start();
    assertTrue(s.awaitIdle(5000));
    assertEquals(2L, s.eventsWritten());
    assertEquals(0L, s.queueSize());
  }

  @Test
  public void unwritableLocationDropsThenRecovers() throws Exception {
    Path blocker = tmp.newFile("blocker").toPath();
    Path active = blocker.resolve("events.jsonl"); // parent is a regular file
    JsonlEventSink s = sink(active, 100);
    s.start();
    s.onEvent(ev(1));
    assertTrue(s.awaitIdle(5000));
    assertEquals(1L, s.eventsDropped());
    assertEquals(0L, s.eventsWritten());

    Files.delete(blocker);
    s.onEvent(ev(2));
    assertTrue(s.awaitIdle(5000));
    assertEquals(1L, s.eventsWritten());
    assertTrue(Files.exists(active));
  }

  @Test
  public void closeWritesQueuedEventsAndRotates() throws Exception {
    Path active = tmp.getRoot().toPath().resolve("events.jsonl");
    JsonlEventSink s = sink(active, 100);
    s.start();
    s.onEvent(ev(1));
    s.close(2000);
    sink = null;

    assertFalse(Files.exists(active));
    try (java.util.stream.Stream<Path> files = Files.list(tmp.getRoot().toPath())) {
      Path closed = files.filter(p -> p.getFileName().toString().startsWith("events-"))
          .findFirst().get();
      assertEquals(1, Files.readAllLines(closed, UTF_8).size());
    }
  }
}
```

- [ ] **Step 2: Run to fail** — `mvn -q -pl agent test -Dtest=JsonlEventSinkTest` → compilation FAILURE.

- [ ] **Step 3: Implement**

`EventSinkStats.java`:
```java
package org.hadooptraffic.sink;

/** Health counters of the events sink, published via JMX. */
public interface EventSinkStats {
  long eventsWritten();

  long eventsDropped();

  long queueSize();

  long filesDropped();
}
```

`JsonlEventSink.java`:
```java
package org.hadooptraffic.sink;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.hadooptraffic.agent.AgentLog;
import org.hadooptraffic.context.Clock;
import org.hadooptraffic.model.TrafficEvent;

/**
 * Writes events as JSON lines to a rotating local file. {@link #onEvent} only offers to a
 * bounded queue (dropping when full); a single daemon thread formats and writes.
 */
public final class JsonlEventSink implements TrafficSink, EventSinkStats {
  private static final int BATCH = 4096;
  private static final long WARN_INTERVAL_MS = 60_000L;

  private final Path activeFile;
  private final BlockingQueue<TrafficEvent> queue;
  private final RollingEventFile file;
  private final String dn;
  private final Clock clock;
  private final Thread writer;
  private final AtomicLong offered = new AtomicLong();
  private final AtomicLong processed = new AtomicLong();
  private final AtomicLong written = new AtomicLong();
  private final AtomicLong dropped = new AtomicLong();
  private volatile boolean running = true;
  private long lastWarnMillis = Long.MIN_VALUE / 2;

  public JsonlEventSink(Path activeFile, int queueCapacity, long maxFileBytes, long rotateMillis,
      int maxFiles, String dn, Clock clock) {
    this.activeFile = activeFile;
    this.queue = new ArrayBlockingQueue<>(queueCapacity);
    this.file = new RollingEventFile(activeFile, maxFileBytes, rotateMillis, maxFiles, clock);
    this.dn = dn;
    this.clock = clock;
    this.writer = new Thread(this::run, "hdfs-traffic-events");
    this.writer.setDaemon(true);
  }

  public void start() {
    writer.start();
  }

  public Path activeFile() {
    return activeFile;
  }

  @Override
  public void onEvent(TrafficEvent e) {
    if (queue.offer(e)) {
      offered.incrementAndGet();
    } else {
      dropped.incrementAndGet();
    }
  }

  private void run() {
    List<TrafficEvent> batch = new ArrayList<>(BATCH);
    while (running || !queue.isEmpty()) {
      try {
        TrafficEvent first = queue.poll(1, TimeUnit.SECONDS);
        if (first != null) {
          batch.add(first);
          queue.drainTo(batch, BATCH - 1);
        }
        writeBatch(batch);
      } catch (InterruptedException e) {
        running = false;
      } catch (Throwable t) {
        warnRateLimited(t); // the writer thread must never die
      } finally {
        processed.addAndGet(batch.size());
        batch.clear();
      }
    }
  }

  private void writeBatch(List<TrafficEvent> batch) {
    try {
      for (TrafficEvent e : batch) {
        file.write(EventJson.format(e, dn));
      }
      file.flush();
    } catch (IOException e) {
      dropped.addAndGet(batch.size());
      file.abandon();
      warnRateLimited(e);
      return;
    }
    written.addAndGet(batch.size());
    try {
      file.maybeRotate();
    } catch (IOException e) {
      file.abandon();
      warnRateLimited(e);
    }
  }

  private void warnRateLimited(Throwable t) {
    long now = clock.currentTimeMillis();
    if (now - lastWarnMillis >= WARN_INTERVAL_MS) {
      lastWarnMillis = now;
      AgentLog.warn("hdfs-traffic-agent: cannot write events to " + activeFile
          + "; dropping events until it recovers", t);
    }
  }

  /** Waits until every accepted event is written (or dropped) and flushed. For tests. */
  public boolean awaitIdle(long timeoutMs) throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (processed.get() < offered.get()) {
      if (System.currentTimeMillis() > deadline) {
        return false;
      }
      Thread.sleep(10);
    }
    return true;
  }

  /** Drains the queue for at most timeoutMs, then closes and rotates the active file. */
  public void close(long timeoutMs) {
    running = false;
    if (writer.isAlive()) {
      try {
        writer.join(timeoutMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    if (writer.isAlive()) {
      return; // still writing: the file is not ours to touch
    }
    try {
      file.close();
    } catch (IOException e) {
      warnRateLimited(e);
    }
  }

  @Override
  public long eventsWritten() {
    return written.get();
  }

  @Override
  public long eventsDropped() {
    return dropped.get();
  }

  @Override
  public long queueSize() {
    return queue.size();
  }

  @Override
  public long filesDropped() {
    return file.filesDropped();
  }
}
```

Note: in `close()` a never-started writer is not alive, so the method goes straight to `file.close()`; queued events of a never-started sink are discarded — acceptable, the sink is always started by the agent.

- [ ] **Step 4: Run** — `mvn -q -pl agent test -Dtest=JsonlEventSinkTest` → PASS (4 tests).

- [ ] **Step 5: Checkpoint** — `git add agent/src && git status --short`.

---

### Task 8: Wire the sink into the agent and JMX

**Files:**
- Modify: `agent/src/main/java/org/hadooptraffic/agent/TrafficAgent.java`
- Modify: `agent/src/main/java/org/hadooptraffic/metrics/MetricsRegistrar.java`
- Modify: `agent/src/main/java/org/hadooptraffic/metrics/UserTrafficMetricsSource.java`
- Test: `agent/src/test/java/org/hadooptraffic/metrics/UserTrafficMetricsSourceTest.java`

**Interfaces:**
- Consumes: `AgentConfig` getters (Task 4), `OpContext.setFlushIntervalSec` (Task 3), `JsonlEventSink`, `EventSinkStats` (Task 7).
- Produces: `TrafficAgent.eventSink(): JsonlEventSink` (null when off); `MetricsRegistrar.init(TrafficAggregator, EventSinkStats)`; `UserTrafficMetricsSource(TrafficAggregator, EventSinkStats)`; `UserTrafficMetricsSource.sinkValues(EventSinkStats): Map<String, Long>` with keys `EventsWritten`, `EventsDropped`, `EventFilesDropped`, `EventsQueueSize`.

- [ ] **Step 1: Write the failing test** — append to `UserTrafficMetricsSourceTest`:
```java
  @Test
  public void exposesSinkHealthCounters() {
    EventSinkStats stats = new EventSinkStats() {
      @Override public long eventsWritten() { return 10; }
      @Override public long eventsDropped() { return 2; }
      @Override public long queueSize() { return 5; }
      @Override public long filesDropped() { return 1; }
    };
    Map<String, Long> m = UserTrafficMetricsSource.sinkValues(stats);
    assertEquals(Long.valueOf(10L), m.get("EventsWritten"));
    assertEquals(Long.valueOf(2L), m.get("EventsDropped"));
    assertEquals(Long.valueOf(1L), m.get("EventFilesDropped"));
    assertEquals(Long.valueOf(5L), m.get("EventsQueueSize"));
  }
```
Add `import org.hadooptraffic.sink.EventSinkStats;`.

- [ ] **Step 2: Run to fail** — `mvn -q -pl agent test -Dtest=UserTrafficMetricsSourceTest` → compilation FAILURE.

- [ ] **Step 3: Implement**

`UserTrafficMetricsSource` — add a field, change the constructor and `getMetrics`, add `sinkValues`:
```java
  private final TrafficAggregator aggregator;
  private final EventSinkStats sinkStats;

  /** @param sinkStats events sink health, or null when the sink is off */
  public UserTrafficMetricsSource(TrafficAggregator aggregator, EventSinkStats sinkStats) {
    this.aggregator = aggregator;
    this.sinkStats = sinkStats;
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder rb = collector.addRecord(RECORD_INFO).setContext("dfs");
    for (Map.Entry<String, Long> e : metricValues(aggregator.snapshot()).entrySet()) {
      rb.addCounter(new MetricName(e.getKey(), describe(e.getKey())), e.getValue());
    }
    if (sinkStats != null) {
      for (Map.Entry<String, Long> e : sinkValues(sinkStats).entrySet()) {
        MetricName name = new MetricName(e.getKey(), describe(e.getKey()));
        if ("EventsQueueSize".equals(e.getKey())) {
          rb.addGauge(name, e.getValue());
        } else {
          rb.addCounter(name, e.getValue());
        }
      }
    }
  }

  /** Global health counters of the events sink. */
  public static Map<String, Long> sinkValues(EventSinkStats s) {
    Map<String, Long> out = new LinkedHashMap<>();
    out.put("EventsWritten", s.eventsWritten());
    out.put("EventsDropped", s.eventsDropped());
    out.put("EventFilesDropped", s.filesDropped());
    out.put("EventsQueueSize", s.queueSize());
    return out;
  }
```
Extend `describe`:
```java
  private static String describe(String name) {
    switch (name) {
      case "EventsWritten":
        return "Events written to the local JSONL file";
      case "EventsDropped":
        return "Events lost: full queue or I/O error";
      case "EventFilesDropped":
        return "Closed event files deleted before shipping (eventsMaxFiles)";
      case "EventsQueueSize":
        return "Events waiting to be written";
      default:
        break;
    }
    if (name.startsWith("EstimatedBytes.")) {
      return "Estimated bytes (short-circuit reads, not observed on the wire)";
    }
    if (name.startsWith("Bytes.")) {
      return "Payload bytes transferred";
    }
    return "Operations";
  }
```
Add `import org.hadooptraffic.sink.EventSinkStats;`.

`MetricsRegistrar`:
```java
  private static volatile TrafficAggregator aggregator;
  private static volatile EventSinkStats sinkStats;

  /** Called by the agent at install time; sinkStats is null when the events sink is off. */
  public static void init(TrafficAggregator agg, EventSinkStats stats) {
    aggregator = agg;
    sinkStats = stats;
  }
```
and in `registerOnce` use `new UserTrafficMetricsSource(agg, sinkStats)`. Add the import.

`TrafficAgent` — replace the class body:
```java
package org.hadooptraffic.agent;

import java.lang.instrument.Instrumentation;
import java.net.InetAddress;
import java.nio.file.Paths;
import org.hadooptraffic.aggregate.TrafficAggregator;
import org.hadooptraffic.context.Clock;
import org.hadooptraffic.context.OpContext;
import org.hadooptraffic.metrics.MetricsRegistrar;
import org.hadooptraffic.sink.JsonlEventSink;
import org.hadooptraffic.sink.TrafficSinks;

/** Entry point: -javaagent:hdfs-traffic-agent.jar[=key=value,...]. */
public final class TrafficAgent {
  private static volatile TrafficAggregator aggregator;
  private static volatile JsonlEventSink eventSink;
  private static boolean installed;

  private TrafficAgent() {
  }

  public static void premain(String args, Instrumentation inst) {
    install(args, inst);
  }

  public static void agentmain(String args, Instrumentation inst) {
    install(args, inst);
  }

  /** Idempotent. Never throws: any failure disables the agent and is logged. */
  public static synchronized void install(String args, Instrumentation inst) {
    if (installed) {
      return;
    }
    installed = true;
    try {
      AgentConfig config = AgentConfig.parse(args);
      OpContext.setFlushIntervalSec(config.flushIntervalSec());
      TrafficAggregator agg = new TrafficAggregator(config.maxUsers());
      TrafficSinks.register(agg);
      JsonlEventSink sink = null;
      if (config.eventsPath() != null) {
        sink = new JsonlEventSink(Paths.get(config.eventsPath()), config.eventsQueue(),
            config.eventsMaxFileMb() * 1024L * 1024L, config.eventsRotateSec() * 1000L,
            config.eventsMaxFiles(), localHostName(), Clock.SYSTEM);
        sink.start();
        TrafficSinks.register(sink);
        JsonlEventSink toClose = sink;
        Runtime.getRuntime().addShutdownHook(
            new Thread(() -> toClose.close(2000), "hdfs-traffic-events-shutdown"));
      }
      MetricsRegistrar.init(agg, sink);
      aggregator = agg;
      eventSink = sink;
      Instrumentations.builder().installOn(inst);
      AgentLog.info("hdfs-traffic-agent installed (maxUsers=" + config.maxUsers()
          + ", events=" + (sink == null ? "off" : config.eventsPath())
          + ", flushIntervalSec=" + config.flushIntervalSec() + ")");
    } catch (Throwable t) {
      AgentLog.warn("hdfs-traffic-agent disabled: " + t, t);
    }
  }

  private static String localHostName() {
    try {
      return InetAddress.getLocalHost().getCanonicalHostName();
    } catch (Exception e) {
      return "unknown";
    }
  }

  /** The aggregator, or null if the agent is not installed. */
  public static TrafficAggregator aggregator() {
    return aggregator;
  }

  /** The JSONL events sink, or null if the agent is not installed or events are off. */
  public static JsonlEventSink eventSink() {
    return eventSink;
  }
}
```

- [ ] **Step 4: Run the unit suite** — `mvn -q -pl agent test -Dtest='*Test'` → PASS.

- [ ] **Step 5: Checkpoint** — `git add agent/src && git status --short`.

---

### Task 9: WebHDFS path capture

**Files:**
- Create: `agent/src/main/java/org/hadooptraffic/hooks/WebHdfsPaths.java`
- Modify: `agent/src/main/java/org/hadooptraffic/hooks/WebHdfsHooks.java` (`onRequestEnter`)
- Modify: `agent/src/main/java/org/hadooptraffic/advice/WebHdfsRequestAdvice.java`
- Test: `agent/src/test/java/org/hadooptraffic/hooks/WebHdfsHooksTest.java`

**Interfaces:**
- Consumes: `OpContext.setPath` (Task 3).
- Produces: `WebHdfsHooks.onRequestEnter(String method, UserGroupInformation ugi, ChannelHandlerContext ctx, Object handler)` (replaces the 3-arg version); `WebHdfsPaths.of(Object handler): String` (package-private).

Background: `WebHdfsHandler.channelRead0` sets its private `String path` field (`params.path()`) before calling `onOpen` / `onCreate` / `onAppend`. `javap -p` on the 3.1.1 and 3.3.6 jars shows `private java.lang.String path;` in both. Reading it through `@Advice.FieldValue` would disable the whole WebHDFS advice if a build lacked it, so it is read reflectively and failures only disable the path.

- [ ] **Step 1: Write the failing tests**

In `WebHdfsHooksTest`, change the 4 existing calls `WebHdfsHooks.onRequestEnter("...", erin, null)` to `WebHdfsHooks.onRequestEnter("...", erin, null, null)`, then add:
```java
  /** Stand-in for WebHdfsHandler: only the private field matters. */
  static final class FakeHandler {
    @SuppressWarnings("unused")
    private String path = "/user/erin/data.bin";
  }

  @Test
  public void requestPathIsCarriedToTheEvent() {
    Object writer = new Object();
    boolean started = WebHdfsHooks.onRequestEnter("onCreate", erin, null, new FakeHandler());
    WebHdfsHooks.bindPending(writer);
    WebHdfsHooks.onRequestExit(started);
    WebHdfsHooks.addBytes(writer, 10);
    WebHdfsHooks.finish(writer);

    assertEquals("/user/erin/data.bin", events.get(0).getPath());
  }

  @Test
  public void handlerWithoutPathFieldLeavesPathNull() {
    assertNull(WebHdfsPaths.of(new Object()));
    assertNull(WebHdfsPaths.of(null));
    assertEquals("/user/erin/data.bin", WebHdfsPaths.of(new FakeHandler()));
  }
```

- [ ] **Step 2: Run to fail** — `mvn -q -pl agent test -Dtest=WebHdfsHooksTest` → compilation FAILURE.

- [ ] **Step 3: Implement**

`WebHdfsPaths.java`:
```java
package org.hadooptraffic.hooks;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicBoolean;
import org.hadooptraffic.agent.AgentLog;

/** Reads WebHdfsHandler's private "path" field; any failure only disables path capture. */
final class WebHdfsPaths {
  private static final AtomicBoolean WARNED = new AtomicBoolean();
  private static volatile Lookup cached;

  private WebHdfsPaths() {
  }

  /** Field lookup result per handler class; field is null when the class has none. */
  private static final class Lookup {
    final Class<?> owner;
    final Field field;

    Lookup(Class<?> owner, Field field) {
      this.owner = owner;
      this.field = field;
    }
  }

  static String of(Object handler) {
    if (handler == null) {
      return null;
    }
    Lookup l = cached;
    if (l == null || l.owner != handler.getClass()) {
      l = new Lookup(handler.getClass(), find(handler.getClass()));
      cached = l;
    }
    if (l.field == null) {
      return null;
    }
    try {
      Object v = l.field.get(handler);
      return v instanceof String ? (String) v : null;
    } catch (Throwable t) {
      warnOnce(t);
      return null;
    }
  }

  private static Field find(Class<?> c) {
    try {
      Field f = c.getDeclaredField("path");
      f.setAccessible(true);
      return f;
    } catch (Throwable t) {
      if (c.getName().endsWith("WebHdfsHandler")) {
        warnOnce(t); // only the real handler is expected to have it
      }
      return null;
    }
  }

  private static void warnOnce(Throwable t) {
    if (WARNED.compareAndSet(false, true)) {
      AgentLog.warn("hdfs-traffic-agent: WebHDFS path capture is disabled", t);
    }
  }
}
```

`WebHdfsHooks.onRequestEnter` — replace:
```java
  public static boolean onRequestEnter(String method, UserGroupInformation ugi,
      ChannelHandlerContext ctx, Object handler) {
    Op op = Op.fromWebHdfsMethod(method);
    if (op == null) {
      return false;
    }
    String user = ugi == null ? UserResolver.UNKNOWN : ugi.getShortUserName();
    SocketAddress remote = ctx == null ? null : ctx.channel().remoteAddress();
    OpContext c = new OpContext(user, op, Role.CLIENT, LocalityResolver.of(remote), null, -1L,
        remote == null ? null : remote.toString());
    c.setPath(WebHdfsPaths.of(handler));
    PENDING.set(c);
    return true;
  }
```

`WebHdfsRequestAdvice.enter` — replace:
```java
  @Advice.OnMethodEnter(suppress = Throwable.class)
  public static boolean enter(@Advice.Origin("#m") String method,
      @Advice.This Object handler,
      @Advice.FieldValue("ugi") UserGroupInformation ugi,
      @Advice.Argument(0) ChannelHandlerContext ctx) {
    return WebHdfsHooks.onRequestEnter(method, ugi, ctx, handler);
  }
```

- [ ] **Step 4: Run** — `mvn -q -pl agent test -Dtest='WebHdfsHooksTest'` → PASS.

- [ ] **Step 5: Checkpoint** — `git add agent/src && git status --short`.

---

### Task 10: Cluster ITs for the event stream

**Files:**
- Modify: `agent/src/test/java/org/hadooptraffic/it/TestAgent.java`
- Create: `agent/src/test/java/org/hadooptraffic/it/EventFiles.java`
- Create: `agent/src/test/java/org/hadooptraffic/it/EventsIT.java`

**Interfaces:**
- Consumes: `TrafficAgent.eventSink()`, `JsonlEventSink.awaitIdle/activeFile` (Tasks 7–8), `ClusterTestBase`, `TrafficAssert`, `Await`.
- Produces: `TestAgent.eventsFile(): Path`; `EventFiles.read(): List<JsonNode>`, `EventFiles.bytes(List<JsonNode>, String user, String op, String role): long`, `EventFiles.lastCount(...)`, `EventFiles.paths(List<JsonNode>, String user, String op): Set<String>`.

`com.fasterxml.jackson.databind` is on the test classpath through `hadoop-common` (Jackson 2 in both 3.1.1 and 3.3.6); it is only used in tests.

- [ ] **Step 1: Turn events on for every IT JVM** — replace `TestAgent`:
```java
package org.hadooptraffic.it;

import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.nio.file.Paths;
import net.bytebuddy.agent.ByteBuddyAgent;
import org.hadooptraffic.agent.TrafficAgent;

/** Installs the agent into the test JVM (idempotent), with JSONL events on. */
public final class TestAgent {
  private static final Path EVENTS = Paths.get("target", "it-events",
      ManagementFactory.getRuntimeMXBean().getName().replaceAll("[^A-Za-z0-9_.-]", "_"),
      "events.jsonl").toAbsolutePath();

  private TestAgent() {
  }

  public static Path eventsFile() {
    return EVENTS;
  }

  public static void install() {
    TrafficAgent.install("maxUsers=100,events=" + EVENTS + ",flushIntervalSec=1",
        ByteBuddyAgent.install());
  }
}
```

- [ ] **Step 2: Add the reader** — `EventFiles.java`:
```java
package org.hadooptraffic.it;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.hadooptraffic.agent.TrafficAgent;

/** Reads the JSONL events the agent wrote in this JVM (active and closed files). */
public final class EventFiles {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private EventFiles() {
  }

  public static List<JsonNode> read() throws Exception {
    if (!TrafficAgent.eventSink().awaitIdle(10_000)) {
      throw new AssertionError("events sink did not drain in 10 s");
    }
    Path dir = TestAgent.eventsFile().getParent();
    List<JsonNode> out = new ArrayList<>();
    try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir, "events*.jsonl")) {
      for (Path p : ds) {
        for (String line : Files.readAllLines(p, UTF_8)) {
          if (!line.isEmpty()) {
            out.add(MAPPER.readTree(line));
          }
        }
      }
    }
    return out;
  }

  private static boolean matches(JsonNode e, String user, String op, String role) {
    return e.get("user").asText().equals(user) && e.get("op").asText().equals(op)
        && (role == null || e.get("role").asText().equals(role));
  }

  public static long bytes(List<JsonNode> events, String user, String op, String role) {
    long sum = 0;
    for (JsonNode e : events) {
      if (matches(e, user, op, role)) {
        sum += e.get("bytes").asLong();
      }
    }
    return sum;
  }

  public static long lastCount(List<JsonNode> events, String user, String op, String role) {
    long n = 0;
    for (JsonNode e : events) {
      if (matches(e, user, op, role) && e.get("last").asBoolean()) {
        n++;
      }
    }
    return n;
  }

  public static Set<String> paths(List<JsonNode> events, String user, String op) {
    Set<String> out = new HashSet<>();
    for (JsonNode e : events) {
      if (matches(e, user, op, null) && !e.get("path").isNull()) {
        out.add(e.get("path").asText());
      }
    }
    return out;
  }
}
```

- [ ] **Step 3: Write the IT** — `EventsIT.java`:
```java
package org.hadooptraffic.it;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.net.URI;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.net.NetUtils;
import org.hadooptraffic.model.Op;
import org.hadooptraffic.model.Role;
import org.junit.Test;

public class EventsIT extends ClusterTestBase {

  /** Event sums equal JMX: the aggregator and the file see the same emissions. */
  private static void assertMatchesJmx(String user, Op op, Role role) throws Exception {
    Await.until(() -> {
      try {
        return EventFiles.bytes(EventFiles.read(), user, op.label(), role.label())
            == TrafficAssert.bytes(user, op, role);
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }, AWAIT_MS);
    List<JsonNode> ev = EventFiles.read();
    assertEquals(TrafficAssert.ops(user, op, role),
        EventFiles.lastCount(ev, user, op.label(), role.label()));
  }

  @Test
  public void eventsMatchJmxCounters() throws Exception {
    String writer = "erin-events";
    String reader = "frank-events";
    Path p = new Path(DIR, "events-file");
    as(writer, () -> {
      try (FileSystem fs = FileSystem.newInstance(cluster.getURI(), conf)) {
        DFSTestUtil.createFile(fs, p, FILE_LEN, (short) 3, 11L);
      }
      return null;
    });
    as(reader, () -> {
      try (FileSystem fs = FileSystem.newInstance(cluster.getURI(), conf)) {
        DFSTestUtil.readFile(fs, p);
      }
      return null;
    });
    Await.until(() -> TrafficAssert.bytes(writer, Op.WRITE, Role.CLIENT) == FILE_LEN
        && TrafficAssert.bytes(writer, Op.WRITE, Role.PIPELINE) == 2L * FILE_LEN
        && TrafficAssert.bytes(reader, Op.READ, Role.CLIENT) == FILE_LEN, AWAIT_MS);

    assertMatchesJmx(writer, Op.WRITE, Role.CLIENT);
    assertMatchesJmx(writer, Op.WRITE, Role.PIPELINE);
    assertMatchesJmx(reader, Op.READ, Role.CLIENT);

    for (JsonNode e : EventFiles.read()) {
      if (e.get("user").asText().equals(writer)) {
        assertTrue(e.toString(), e.get("file_block_id").asLong() > 0);
        assertTrue(e.toString(), e.get("op_id").asText().matches("[0-9a-f]{6}-\\d+"));
      }
    }
  }

  @Test
  public void webHdfsEventsCarryThePath() throws Exception {
    String user = "gina-events";
    Path p = new Path(DIR, "gina-web");
    URI web = new URI("webhdfs://"
        + NetUtils.getHostPortString(cluster.getNameNode().getHttpAddress()));
    as(user, () -> {
      try (FileSystem fs = FileSystem.newInstance(web, conf)) {
        DFSTestUtil.createFile(fs, p, FILE_LEN, (short) 1, 12L);
        DFSTestUtil.readFile(fs, p);
      }
      return null;
    });
    Await.until(() -> TrafficAssert.bytes(user, Op.WEBHDFS_CREATE, Role.CLIENT) == FILE_LEN
        && TrafficAssert.bytes(user, Op.WEBHDFS_READ, Role.CLIENT) == FILE_LEN, AWAIT_MS);

    List<JsonNode> ev = EventFiles.read();
    assertEquals(java.util.Collections.singleton(p.toString()),
        EventFiles.paths(ev, user, "webhdfs_create"));
    assertEquals(java.util.Collections.singleton(p.toString()),
        EventFiles.paths(ev, user, "webhdfs_read"));
  }
}
```

- [ ] **Step 4: Run all ITs in both profiles**

Run: `mvn -q -pl agent verify -P hadoop-3.3.6 && mvn -q -pl agent verify -P hadoop-3.1.1`
Expected: BUILD SUCCESS; `EventsIT` 2 tests pass; existing `DataTransferProtocolIT`, `WebHdfsIT`, `KerberosTrafficIT` still pass (they now run with `flushIntervalSec=1`, which exercises deltas against their exact byte/op assertions).
If `webHdfsEventsCarryThePath` fails with an empty set on one profile, check the agent log in `agent/target/surefire-reports/*EventsIT-output.txt` for `WebHDFS path capture is disabled` and stop: the field name differs in that build and the spec must be revisited.

- [ ] **Step 5: Checkpoint** — `git add agent/src && git status --short`.

---

### Task 11: Shipping script and stand check

**Files:**
- Create: `deploy/upload-events.sh`
- Modify: `stand/docker-compose.yml` (`x-datanode`)
- Modify: `stand/hadoop/entrypoint.sh` (datanode branch)
- Modify: `stand/run-scenario.sh` (tail)
- Modify: `stand/check_traffic.py` (`events` mode)

**Interfaces:**
- Produces: `upload-events.sh -d <events dir> -r <HDFS root> [-n <file prefix>] [-k <keytab> -p <principal>]`; layout `<root>/dt=YYYY-MM-DD/dn=<host>/<file>`; `check_traffic.py events SINCE_MS UNTIL_MS` reading JSONL from stdin.

- [ ] **Step 1: Write the script** — `deploy/upload-events.sh` (then `chmod +x`):
```bash
#!/usr/bin/env bash
# Ship closed hdfs-traffic-agent event files from this DataNode into HDFS, then delete them.
# Run from cron every 5 minutes as the user that owns the events directory, e.g.:
#   */5 * * * * hdfs /opt/hdfs-traffic-agent/upload-events.sh -d /var/log/hadoop/hdfs-traffic \
#       -r /data/hdfs-traffic/raw -k /etc/security/keytabs/hdfs.headless.keytab -p hdfs@REALM
# Closed files are immutable and carry their UTC date in the name, so a re-run after a failure
# simply puts the same file again (-f).
set -euo pipefail

EVENTS_DIR=/var/log/hadoop/hdfs-traffic
ROOT=/data/hdfs-traffic/raw
PREFIX=events
KEYTAB=
PRINCIPAL=

usage() {
  echo "usage: $0 [-d events_dir] [-r hdfs_root] [-n file_prefix] [-k keytab -p principal]" >&2
  exit 2
}

while getopts "d:r:n:k:p:" opt; do
  case "${opt}" in
    d) EVENTS_DIR=${OPTARG} ;;
    r) ROOT=${OPTARG} ;;
    n) PREFIX=${OPTARG} ;;
    k) KEYTAB=${OPTARG} ;;
    p) PRINCIPAL=${OPTARG} ;;
    *) usage ;;
  esac
done
if [[ -n "${KEYTAB}" && -z "${PRINCIPAL}" ]]; then
  usage
fi

exec 9>"${EVENTS_DIR}/.upload.lock"
if ! flock -n 9; then
  echo "upload-events: another run is in progress" >&2
  exit 0
fi

if [[ -n "${KEYTAB}" ]]; then
  export KRB5CCNAME="FILE:${EVENTS_DIR}/.upload.krb5cc"
  kinit -kt "${KEYTAB}" "${PRINCIPAL}"
fi

host=$(hostname -f)
shopt -s nullglob
for f in "${EVENTS_DIR}/${PREFIX}"-[0-9]*-[0-9]*-[0-9]*.jsonl; do
  name=$(basename "${f}")
  stamp=${name#"${PREFIX}"-}
  day=${stamp:0:8}
  dest="${ROOT}/dt=${day:0:4}-${day:4:2}-${day:6:2}/dn=${host}"
  hdfs dfs -mkdir -p "${dest}"
  hdfs dfs -put -f "${f}" "${dest}/${name}"
  rm -f "${f}"
  echo "upload-events: ${name} -> ${dest}"
done
```

- [ ] **Step 2: Enable events on the stand**

`stand/docker-compose.yml`, in `x-datanode`:
```yaml
  environment:
    ROLE: datanode
    AGENT_JAR: /opt/agent/hdfs-traffic-agent.jar
    AGENT_ARGS: ${AGENT_ARGS:-maxUsers=2000,events=/var/log/hdfs-traffic/events.jsonl,eventsRotateSec=20,flushIntervalSec=5}
  volumes:
    - keytabs:/keytabs:ro
    - ${AGENT_JAR_HOST:-../agent/target/hdfs-traffic-agent-0.1.0-SNAPSHOT.jar}:/opt/agent/hdfs-traffic-agent.jar:ro
    - ../deploy:/opt/hdfs-traffic-deploy:ro
```

`stand/hadoop/entrypoint.sh`, inside `if [[ "${ROLE}" == "datanode" && -n "${AGENT_JAR:-}" ]]; then`, before the `if [[ -f "${AGENT_JAR}" ]]` line:
```bash
  install -d -o hdfs -g hadoop /var/log/hdfs-traffic
```

- [ ] **Step 3: Add the `events` mode to `check_traffic.py`**

Add after `failures()`:
```python
# WebHDFS events carry the path the DataNode was asked for; `hdfs dfs -put` writes to a
# temporary name first, so the create path is checked by prefix.
PATH_PREFIXES = {
    ("bob", "webhdfs_create"): "/traffic/bob-web.bin",
    ("bob", "webhdfs_read"): "/traffic/bob-web.bin",
}


def events_table(lines, since, until):
    """JSONL events with since <= ts <= until -> same shape as delta(): key -> counters."""
    table, paths = {}, {}
    for line in lines:
        line = line.strip()
        if not line:
            continue
        e = json.loads(line)
        if not since <= e["ts"] <= until:
            continue
        row = table.setdefault((e["user"], e["op"], e["role"], e["locality"]), {})
        row["Bytes"] = row.get("Bytes", 0) + e["bytes"]
        row["EstimatedBytes"] = row.get("EstimatedBytes", 0) + e["est_bytes"]
        if e["last"]:
            row["Ops"] = row.get("Ops", 0) + 1
        if e["path"] is not None:
            paths.setdefault((e["user"], e["op"]), set()).add(e["path"])
    return table, paths


def path_failures(paths):
    bad = []
    for key, prefix in PATH_PREFIXES.items():
        got = paths.get(key, set())
        if not got or not all(p.startswith(prefix) for p in got):
            bad.append("%s: paths %s, expected prefix %s" % (key, sorted(got), prefix))
    return bad
```
In `main()`, before the final `print(__doc__)`:
```python
    if len(sys.argv) == 4 and sys.argv[1] == "events":
        table, paths = events_table(sys.stdin, int(sys.argv[2]), int(sys.argv[3]))
        show(table)
        bad = failures(table) + path_failures(paths)
        if bad:
            print("\nFAIL:\n  " + "\n  ".join(bad))
            return 1
        print("\nOK: events match all %d expectations" % (len(EXPECTED) + len(AT_LEAST)
                                                          + len(PATH_PREFIXES)))
        return 0
```
Update the module docstring:
```
  check_traffic.py events SINCE UNTIL    -> read JSONL events from stdin (ts in [SINCE, UNTIL] ms),
                                            check them against the same expectations
```

- [ ] **Step 4: Extend `run-scenario.sh`**

After the `python3 -I "${STAND_DIR}/check_traffic.py" snapshot > "${WORK}/baseline.json"` line:
```bash
now_ms() { python3 -c 'import time; print(int(time.time() * 1000))'; }
since_ms=$(now_ms)
```
At the end of the file:
```bash
until_ms=$(now_ms)

# Events: every DataNode ships its closed files (rotation every 20 s) into HDFS; compare the
# events of this run with the same expectations. Retry while files are still being rotated.
step "events: shipped to HDFS, summed over the DataNodes"
ok=""
for attempt in 1 2 3 4 5; do
  sleep 15
  for dn in tr-dn1 tr-dn2 tr-dn3; do
    docker exec "${dn}" runuser -u hdfs -- /opt/hdfs-traffic-deploy/upload-events.sh \
      -d /var/log/hdfs-traffic -r /data/hdfs-traffic/raw \
      -k /etc/security/keytabs/host.keytab -p "hdfs/${dn}.traffic-net@${REALM}" >/dev/null
  done
  if as_hdfs 'hdfs dfs -cat "/data/hdfs-traffic/raw/dt=*/dn=*/*.jsonl"' \
      | python3 -I "${STAND_DIR}/check_traffic.py" events "${since_ms}" "${until_ms}" \
      > "${WORK}/events.txt"; then
    ok=1
    break
  fi
done
cat "${WORK}/events.txt"
[[ -n "${ok}" ]] || { echo "FAIL: events in HDFS do not match the scenario" >&2; exit 1; }
```

- [ ] **Step 5: Run the stand (both flavours)**

```bash
mvn -q clean package -P hadoop-3.1.1 -DskipTests
cd stand
docker compose --env-file .env.hdp up -d --build
./run-scenario.sh hdp
docker compose --env-file .env.hdp down -v
```
Expected: existing checks pass, then `OK: events match all 14 expectations`. Repeat with `apache` / `.env.apache` after `mvn -q clean package -P hadoop-3.3.6 -DskipTests`.
If `hdfs` is not on `PATH` under `runuser` in the DataNode container, call it as `${HADOOP_HOME}/bin/hdfs` by adding `export PATH="${HADOOP_HOME}/bin:${PATH}"` at the top of the `docker exec` command (`bash -c`), not in the script.

- [ ] **Step 6: Checkpoint** — `git add deploy stand && git status --short`.

---

### Task 12: Documentation (EN + RU)

**Files:**
- Modify: `README.md`, `README.ru.md`

- [ ] **Step 1: Build section** — EN line 37 → ``The jar is `agent/target/hdfs-traffic-agent-<version>.jar` (same jar for both Hadoop lines).``; RU line 38 → ``Результат — `agent/target/hdfs-traffic-agent-<version>.jar`, один jar для обеих версий Hadoop.``; stand block (EN 109 / RU 112) → `# JDK 8; the stand mounts agent/target/*.jar` / `# JDK 8; стенд монтирует agent/target/*.jar`.

- [ ] **Step 2: Options** — replace the "the only option is `maxUsers`" paragraph (EN ~50, RU ~52) with a table. EN:

```markdown
   Agent options (comma-separated `key=value`; an unknown or invalid option disables the
   agent with a WARN, the DataNode still starts):

   | Option | Default | Meaning |
   |---|---|---|
   | `maxUsers` | 2000 | JMX user slots, first come first served; the rest go to `__other__` |
   | `events` | off | active JSONL events file, e.g. `/var/log/hadoop/hdfs-traffic/events.jsonl` (no commas) |
   | `eventsQueue` | 65536 | events buffered for the writer thread; overflow is dropped and counted |
   | `eventsMaxFileMb` | 256 | rotate the active file at this size |
   | `eventsRotateSec` | 300 | rotate the active file at this age |
   | `eventsMaxFiles` | 100 | closed files kept while not shipped; the oldest are deleted beyond it |
   | `flushIntervalSec` | 10 | long operations report their bytes every N seconds (JMX and events); 0 = at the end only |
```
RU — the same table with translated text:
```markdown
   Опции агента (через запятую, `key=value`; неизвестная или неверная опция отключает агент
   с WARN в логе, сама DataNode запускается):

   | Опция | По умолчанию | Смысл |
   |---|---|---|
   | `maxUsers` | 2000 | слоты пользователей в JMX по принципу «кто первый»; остальные — `__other__` |
   | `events` | выкл. | активный JSONL-файл событий, например `/var/log/hadoop/hdfs-traffic/events.jsonl` (без запятых) |
   | `eventsQueue` | 65536 | буфер событий для потока записи; переполнение отбрасывается со счётчиком |
   | `eventsMaxFileMb` | 256 | ротация активного файла по размеру |
   | `eventsRotateSec` | 300 | ротация активного файла по возрасту |
   | `eventsMaxFiles` | 100 | сколько закрытых неотправленных файлов хранить; старейшие сверх лимита удаляются |
   | `flushIntervalSec` | 10 | долгие операции отчитываются о байтах каждые N секунд (JMX и события); 0 — только в конце |
```

- [ ] **Step 3: New section after "Metrics"** — EN `## Events (JSONL)` / RU `## События (JSONL)` describing: one line per emission with the field list from spec 3.3 (`ts`, `start`, `dn`, `op_id`, `user`, `op`, `role`, `locality`, `bpid`, `block_id`, `file_block_id`, `bytes`, `est_bytes`, `dur_ms`, `last`, `peer`, `path`); `bytes` is a delta, sum over `op_id`; `last=true` closes an operation; `user` is not capped by `maxUsers`; closed files `events-<yyyyMMdd-HHmmss>-<seq>.jsonl` (UTC open time, never across a UTC day); JMX health attributes `EventsWritten`, `EventsDropped`, `EventFilesDropped`, `EventsQueueSize`; shipping with `deploy/upload-events.sh` from cron, with the cron line from the script header and the HDFS layout `<root>/dt=YYYY-MM-DD/dn=<host>/`. Mention that the DataNode user must be able to create the events directory, and that the upload's own writes appear as the service user's traffic.

- [ ] **Step 4: Limitations** — EN: replace the "write bytes are counted when the block is closed" bullet with: "Write bytes are reported every `flushIntervalSec` while a block is open and the rest on close; bytes not yet reported when the DataNode restarts are lost." Replace "Block-to-file mapping is not implemented yet …" with "Events carry block ids only; file paths come from the fsimage-based mapping (block-mapper, see `docs/superpowers/specs/2026-11-06-block-to-file-mapping-design.md`)." RU — same two bullets: «Байты записи отражаются каждые `flushIntervalSec`, пока блок открыт, остаток — при закрытии; байты, ещё не отражённые к моменту рестарта DataNode, теряются.» и «События содержат только id блоков; пути файлов даёт сопоставление по fsimage (block-mapper, см. `docs/superpowers/specs/2026-11-06-block-to-file-mapping-design.md`).»

- [ ] **Step 5: Stand paragraph** — EN/RU: add a sentence that `run-scenario.sh` also ships the events with `upload-events.sh` and checks them against the same expected byte counts and the WebHDFS paths.

- [ ] **Step 6: Checkpoint** — `git add README.md README.ru.md && git diff --cached --stat`. Report to the user; commit only on their command.
