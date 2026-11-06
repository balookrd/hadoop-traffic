# HDFS DataNode Per-User Traffic Agent Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A Java agent for HDFS DataNodes (Apache Hadoop 3.3.x and HDP 3.1 / Hadoop 3.1.1) that accounts transferred bytes and operation counts per user, op, role and locality, and exposes them via metrics2 / JMX.

**Architecture:** A ByteBuddy agent (`-javaagent`) inlines thin advice into `DataXceiver`, `BlockSender`, `BlockReceiver`, `DataNode$DataTransfer`, `DataNode` and the WebHDFS Netty handlers. The advice delegates to plain-Java "hooks" that keep a per-operation `OpContext`, take the user from the block token (or from the WebHDFS UGI), and emit a `TrafficEvent` to pluggable `TrafficSink`s. The first sink, `TrafficAggregator`, keeps cumulative counters that a metrics2 source publishes as `Hadoop:service=DataNode,name=UserTraffic`.

**Tech Stack:** Java 8, Maven, ByteBuddy 1.14.19 (shaded), Hadoop 3.1.1 / 3.3.6 (`provided`), JUnit 4.13.2, MiniDFSCluster, MiniKdc.

Spec: `docs/superpowers/specs/2026-11-06-hdfs-user-traffic-design.md`.

## Global Constraints

- Bytecode target Java 8 (`maven.compiler.source/target = 1.8`). Build and tests run on **JDK 8**: every `mvn` command below assumes `export JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-1.8.0_432` in the shell (full JDK with `tools.jar`, verified with Maven 3.9.16; `/usr/libexec/java_home` does not list it). Hadoop 3.1/3.3 do not work on JDK 24+ (`Subject.getSubject`), so the Homebrew JDK 25/26 must not be used.
- Hadoop lines: Maven profile `hadoop-3.3.6` (default) and `hadoop-3.1.1`. Every task's tests must pass in **both** profiles before the task is done.
- Hadoop and Netty are `provided`; only ByteBuddy is bundled, relocated to `org.hadooptraffic.shaded.net.bytebuddy`.
- Root package `org.hadooptraffic`.
- Agent must never break a DataNode operation: every advice uses `suppress = Throwable.class`; sinks are isolated by try/catch; a bad agent config disables the agent instead of failing the JVM.
- Metric/label vocabulary (exact strings): ops `read, write, transfer, copy, replace, scr_fds, webhdfs_read, webhdfs_create, webhdfs_append`; roles `client, pipeline, datanode, webhdfs_proxy`; localities `local, remote`; unknown user `__unknown__`; overflow user `__other__`; WebHDFS task id `WEBHDFS` (client name prefix `DFSClient_WEBHDFS_`).
- JMX attribute names: `<Counter>;user=<u>;op=<op>;role=<role>;locality=<loc>` where `<Counter>` ∈ `Bytes, EstimatedBytes, Ops`.
- Agent option string: comma-separated `key=value`; only `maxUsers` (default `2000`, must be ≥ 1).
- Code comments, commit messages: English. **No `git commit` / `git push` without an explicit user command** — every "Checkpoint" step only shows `git status` / `git diff --stat` to the user. Work on `main`, no feature branches.
- Test classes that start a MiniDFSCluster are named `*IT` and run in surefire with `reuseForks=false` (one JVM per test class).

## Verified Hadoop internals (3.1.0 and 3.3.6 bytecode, identical)

- `DataXceiver`: fields `private Peer peer`; public ops
  `readBlock(ExtendedBlock, Token, String clientName, long, long, boolean, CachingStrategy)`,
  `writeBlock(ExtendedBlock, StorageType, Token, String clientname, DatanodeInfo[] targets, StorageType[], DatanodeInfo srcDataNode, BlockConstructionStage, int pipelineSize, long, long, long, DataChecksum, CachingStrategy, boolean, boolean, boolean[], String, String[])`,
  `transferBlock(ExtendedBlock, Token, String clientName, DatanodeInfo[], StorageType[], String[])`,
  `copyBlock(ExtendedBlock, Token)`,
  `replaceBlock(ExtendedBlock, StorageType, Token, String delHint, DatanodeInfo, String)`,
  `requestShortCircuitFds(ExtendedBlock, Token, SlotId, int, boolean)`.
- `BlockSender.sendPacket(ByteBuffer, int, OutputStream, boolean, DataTransferThrottler)` is `private int` and returns `dataLen` (payload bytes).
- `BlockReceiver.receivePacket()` is `private int`, returns payload `len`, `-1` for the last packet, `0` for heartbeats.
- `DataNode$DataTransfer`: fields `final DatanodeInfo[] targets`, `final ExtendedBlock b`, `final String clientname`; `public void run()`.
- `DataNode.startDataNode(List, SecureResources)` (package-private, called from the constructor after `DefaultMetricsSystem.initialize("DataNode")`).
- `WebHdfsHandler`: field `private UserGroupInformation ugi`; `private void onOpen/onCreate/onAppend(ChannelHandlerContext)`; `private static DFSClient newDfsClient(String, Configuration)`; `onCreate` uses field `confForCreate`. Read path wraps data in anonymous `WebHdfsHandler$2 extends io.netty.handler.stream.ChunkedStream` (overrides `close()`).
- `HdfsWriter` (package-private): ctor `(DFSClient, OutputStream, DefaultHttpResponse)`, `protected void channelRead0(ChannelHandlerContext, HttpContent)`, `private void releaseDfsResources()`.
- `DFSClient` name = `"DFSClient_" + conf.get("mapreduce.task.attempt.id", "NONMAPREDUCE") + "_" + random + "_" + threadId`.
- `ChunkedStream.transferredBytes()` exists in Netty 4.0.52 (Hadoop 3.1) and 4.1.x (Hadoop 3.3).
- Hadoop's DataStreamer sends `pipelineSize = nodes.length` and `Sender` strips the first target, so the receiving DN's pipeline position is `pipelineSize - 1 - targets.length`. `DataTransfer` sends `pipelineSize = 0` with stages `TRANSFER_RBW` / `TRANSFER_FINALIZED`.

## Spec refinements made while planning

1. Dimensions are encoded into metric **names** (nntop precedent), not tags: metrics2 flattens multi-record sources into `Bytes`, `Bytes.1`, … in JMX and Ambari Metrics ignores tags.
2. `webhdfs_proxy` applies only to the head of a write pipeline (position 0) and to reads; downstream pipeline DNs stay `pipeline`.
3. Empty `clientName` → `datanode` for every op (EC reconstruction, DN-initiated transfers).
4. Integration tests mostly use simple auth + block tokens (identical code path for user extraction); Kerberos is covered by one smoke IT based on Hadoop's `SaslDataTransferTestCase`.
5. HTTP-leg read bytes are taken from `ChunkedStream.transferredBytes()` on `close()` (no Netty-wide instrumentation).

## Prerequisites (user actions)

- JDK 8: already present at `/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-1.8.0_432` (Liberica 1.8.0_432, includes `lib/tools.jar`).
- The directory is not a git repository. Ask the user whether to run `git init`; checkpoints below assume it exists but never commit.

## File Structure

```
pom.xml
README.md / README.ru.md
src/main/java/org/hadooptraffic/
  model/      Op, Role, Locality, TrafficEvent          — vocabulary + immutable event
  sink/       TrafficSink, TrafficSinks                 — sink SPI + global fan-out
  aggregate/  TrafficKey, TrafficCounters, TrafficAggregator — cumulative counters, user cap
  metrics/    MetricName, UserTrafficMetricsSource, MetricsRegistrar — metrics2 / JMX
  context/    Args, UserResolver, RoleResolver, LocalityResolver, OpContext, OpContexts
  hooks/      XceiverHooks, WebHdfsHooks                — testable logic called by advice
  advice/     *Advice                                   — thin ByteBuddy advice classes
  agent/      AgentConfig, AgentLog, TrafficAgent, Instrumentations, AgentLogListener
src/test/java/org/hadooptraffic/
  (unit tests mirror main packages)
  it/         TestAgent, Await, TrafficAssert, TestTokens, ClusterTestBase,
              DataTransferProtocolIT, WebHdfsIT, KerberosTrafficIT
```

---

### Task 1: Project scaffold, vocabulary, event and sinks

**Files:**
- Create: `pom.xml`
- Create: `src/main/java/org/hadooptraffic/model/Op.java`, `Role.java`, `Locality.java`, `TrafficEvent.java`
- Create: `src/main/java/org/hadooptraffic/sink/TrafficSink.java`, `TrafficSinks.java`
- Test: `src/test/java/org/hadooptraffic/model/OpTest.java`, `src/test/java/org/hadooptraffic/sink/TrafficSinksTest.java`

**Interfaces:**
- Produces: `Op.fromXceiverMethod(String)`, `Op.fromWebHdfsMethod(String)`, `Op/Role/Locality.label()`;
  `TrafficEvent(String user, Op op, Role role, Locality locality, String blockPoolId, long blockId, long bytes, long estimatedBytes, long durationMs, String peer)` with getters `getUser() getOp() getRole() getLocality() getBlockPoolId() getBlockId() getBytes() getEstimatedBytes() getDurationMs() getPeer()`;
  `interface TrafficSink { void onEvent(TrafficEvent e); }`;
  `TrafficSinks.register(TrafficSink)`, `TrafficSinks.emit(TrafficEvent)`, `TrafficSinks.clear()`.

- [ ] **Step 1: Create `pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>

  <groupId>org.hadooptraffic</groupId>
  <artifactId>hdfs-traffic-agent</artifactId>
  <version>0.1.0-SNAPSHOT</version>
  <packaging>jar</packaging>

  <properties>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    <maven.compiler.source>1.8</maven.compiler.source>
    <maven.compiler.target>1.8</maven.compiler.target>
    <hadoop.version>3.3.6</hadoop.version>
    <bytebuddy.version>1.14.19</bytebuddy.version>
    <junit.version>4.13.2</junit.version>
  </properties>

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
      <groupId>org.apache.hadoop</groupId>
      <artifactId>hadoop-minikdc</artifactId>
      <version>${hadoop.version}</version>
      <scope>test</scope>
    </dependency>
  </dependencies>

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
    </plugins>
  </build>
</project>
```

- [ ] **Step 2: Write the failing tests**

`src/test/java/org/hadooptraffic/model/OpTest.java`:

```java
package org.hadooptraffic.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class OpTest {
  @Test
  public void mapsXceiverMethods() {
    assertEquals(Op.READ, Op.fromXceiverMethod("readBlock"));
    assertEquals(Op.WRITE, Op.fromXceiverMethod("writeBlock"));
    assertEquals(Op.TRANSFER, Op.fromXceiverMethod("transferBlock"));
    assertEquals(Op.COPY, Op.fromXceiverMethod("copyBlock"));
    assertEquals(Op.REPLACE, Op.fromXceiverMethod("replaceBlock"));
    assertEquals(Op.SCR_FDS, Op.fromXceiverMethod("requestShortCircuitFds"));
    assertNull(Op.fromXceiverMethod("blockChecksum"));
    assertNull(Op.fromXceiverMethod(null));
  }

  @Test
  public void mapsWebHdfsMethods() {
    assertEquals(Op.WEBHDFS_READ, Op.fromWebHdfsMethod("onOpen"));
    assertEquals(Op.WEBHDFS_CREATE, Op.fromWebHdfsMethod("onCreate"));
    assertEquals(Op.WEBHDFS_APPEND, Op.fromWebHdfsMethod("onAppend"));
    assertNull(Op.fromWebHdfsMethod("onGetFileChecksum"));
  }

  @Test
  public void labelsAreStable() {
    assertEquals("scr_fds", Op.SCR_FDS.label());
    assertEquals("webhdfs_create", Op.WEBHDFS_CREATE.label());
    assertEquals("webhdfs_proxy", Role.WEBHDFS_PROXY.label());
    assertEquals("remote", Locality.REMOTE.label());
  }
}
```

`src/test/java/org/hadooptraffic/sink/TrafficSinksTest.java`:

```java
package org.hadooptraffic.sink;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.hadooptraffic.model.Locality;
import org.hadooptraffic.model.Op;
import org.hadooptraffic.model.Role;
import org.hadooptraffic.model.TrafficEvent;
import org.junit.After;
import org.junit.Test;

public class TrafficSinksTest {
  @After
  public void tearDown() {
    TrafficSinks.clear();
  }

  @Test
  public void emitReachesAllSinksEvenIfOneFails() {
    List<TrafficEvent> got = new ArrayList<>();
    TrafficSinks.register(e -> {
      throw new RuntimeException("boom");
    });
    TrafficSinks.register(got::add);
    TrafficEvent e = sample();

    TrafficSinks.emit(e);

    assertEquals(Collections.singletonList(e), got);
  }

  @Test
  public void emitWithoutSinksIsNoop() {
    TrafficSinks.emit(sample());
  }

  static TrafficEvent sample() {
    return new TrafficEvent("alice", Op.READ, Role.CLIENT, Locality.LOCAL,
        "bp-1", 1L, 10L, 0L, 5L, "127.0.0.1:1");
  }
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `mvn -q test -Dtest='OpTest,TrafficSinksTest'`
Expected: compilation FAILURE (`cannot find symbol: class Op`).

- [ ] **Step 4: Implement**

`src/main/java/org/hadooptraffic/model/Op.java`:

```java
package org.hadooptraffic.model;

/** Kind of DataNode operation being accounted. */
public enum Op {
  READ("read"),
  WRITE("write"),
  TRANSFER("transfer"),
  COPY("copy"),
  REPLACE("replace"),
  SCR_FDS("scr_fds"),
  WEBHDFS_READ("webhdfs_read"),
  WEBHDFS_CREATE("webhdfs_create"),
  WEBHDFS_APPEND("webhdfs_append");

  private final String label;

  Op(String label) {
    this.label = label;
  }

  /** Stable lower-case name used in metric names. */
  public String label() {
    return label;
  }

  /** Maps a DataXceiver method name to an op, or null if the method is not accounted. */
  public static Op fromXceiverMethod(String method) {
    if (method == null) {
      return null;
    }
    switch (method) {
      case "readBlock": return READ;
      case "writeBlock": return WRITE;
      case "transferBlock": return TRANSFER;
      case "copyBlock": return COPY;
      case "replaceBlock": return REPLACE;
      case "requestShortCircuitFds": return SCR_FDS;
      default: return null;
    }
  }

  /** Maps a WebHdfsHandler method name to an op, or null if the method is not accounted. */
  public static Op fromWebHdfsMethod(String method) {
    if (method == null) {
      return null;
    }
    switch (method) {
      case "onOpen": return WEBHDFS_READ;
      case "onCreate": return WEBHDFS_CREATE;
      case "onAppend": return WEBHDFS_APPEND;
      default: return null;
    }
  }
}
```

`src/main/java/org/hadooptraffic/model/Role.java`:

```java
package org.hadooptraffic.model;

/** Who is on the other side of the transfer, from the DataNode's point of view. */
public enum Role {
  /** An HDFS client (or the first DN of a client write pipeline). */
  CLIENT("client"),
  /** A downstream DN in a client write pipeline (replica forwarding). */
  PIPELINE("pipeline"),
  /** DataNode-initiated traffic: re-replication, balancer, pipeline recovery. */
  DATANODE("datanode"),
  /** Internal leg of a WebHDFS request (DN-embedded DFSClient). */
  WEBHDFS_PROXY("webhdfs_proxy");

  private final String label;

  Role(String label) {
    this.label = label;
  }

  public String label() {
    return label;
  }
}
```

`src/main/java/org/hadooptraffic/model/Locality.java`:

```java
package org.hadooptraffic.model;

/** Whether the peer runs on the same host as the DataNode. */
public enum Locality {
  LOCAL("local"),
  REMOTE("remote");

  private final String label;

  Locality(String label) {
    this.label = label;
  }

  public String label() {
    return label;
  }
}
```

`src/main/java/org/hadooptraffic/model/TrafficEvent.java`:

```java
package org.hadooptraffic.model;

/** One finished DataNode operation. Immutable. */
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

  public TrafficEvent(String user, Op op, Role role, Locality locality, String blockPoolId,
      long blockId, long bytes, long estimatedBytes, long durationMs, String peer) {
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
  }

  public String getUser() { return user; }
  public Op getOp() { return op; }
  public Role getRole() { return role; }
  public Locality getLocality() { return locality; }
  /** Block pool id, or null for WebHDFS HTTP-leg events. */
  public String getBlockPoolId() { return blockPoolId; }
  /** Block id, or -1 when not applicable. Kept for block-to-file mapping (step 2). */
  public long getBlockId() { return blockId; }
  public long getBytes() { return bytes; }
  public long getEstimatedBytes() { return estimatedBytes; }
  public long getDurationMs() { return durationMs; }
  public String getPeer() { return peer; }

  @Override
  public String toString() {
    return "TrafficEvent{user=" + user + ", op=" + op.label() + ", role=" + role.label()
        + ", locality=" + locality.label() + ", bpid=" + blockPoolId + ", blockId=" + blockId
        + ", bytes=" + bytes + ", estimatedBytes=" + estimatedBytes
        + ", durationMs=" + durationMs + ", peer=" + peer + '}';
  }
}
```

`src/main/java/org/hadooptraffic/sink/TrafficSink.java`:

```java
package org.hadooptraffic.sink;

import org.hadooptraffic.model.TrafficEvent;

/** Receives finished operations. Implementations must be thread-safe and fast. */
public interface TrafficSink {
  void onEvent(TrafficEvent event);
}
```

`src/main/java/org/hadooptraffic/sink/TrafficSinks.java`:

```java
package org.hadooptraffic.sink;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import org.hadooptraffic.model.TrafficEvent;

/** Process-wide fan-out of events to registered sinks. */
public final class TrafficSinks {
  private static final List<TrafficSink> SINKS = new CopyOnWriteArrayList<>();

  private TrafficSinks() {
  }

  public static void register(TrafficSink sink) {
    SINKS.add(Objects.requireNonNull(sink, "sink"));
  }

  /** Removes all sinks. Intended for tests. */
  public static void clear() {
    SINKS.clear();
  }

  public static void emit(TrafficEvent event) {
    for (TrafficSink sink : SINKS) {
      try {
        sink.onEvent(event);
      } catch (RuntimeException e) {
        // A faulty sink must never break a DataNode operation or starve other sinks.
      }
    }
  }
}
```

- [ ] **Step 5: Run tests to verify they pass in both profiles**

Run: `mvn -q test -Dtest='OpTest,TrafficSinksTest' && mvn -q -P hadoop-3.1.1 test -Dtest='OpTest,TrafficSinksTest'`
Expected: both runs succeed (no output with `-q`, exit code 0). The first 3.1.1 run downloads Hadoop 3.1.1 artifacts.

- [ ] **Step 6: Checkpoint** — show `git status` to the user; do not commit unless told to. Suggested message: `feat: project scaffold, traffic vocabulary and sinks`.

---

### Task 2: Aggregator with user cap

**Files:**
- Create: `src/main/java/org/hadooptraffic/aggregate/TrafficKey.java`, `TrafficCounters.java`, `TrafficAggregator.java`
- Test: `src/test/java/org/hadooptraffic/aggregate/TrafficAggregatorTest.java`

**Interfaces:**
- Consumes: `TrafficEvent`, `TrafficSink`, `Op`, `Role`, `Locality` (Task 1).
- Produces: `TrafficKey(String user, Op op, Role role, Locality locality)` + getters `getUser() getOp() getRole() getLocality()`;
  `TrafficCounters.Snapshot` with `getBytes() getEstimatedBytes() getOps()`;
  `TrafficAggregator(int maxUsers) implements TrafficSink`, `Map<TrafficKey, TrafficCounters.Snapshot> snapshot()`, constant `TrafficAggregator.OTHER_USER = "__other__"`.

- [ ] **Step 1: Write the failing test**

```java
package org.hadooptraffic.aggregate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import org.hadooptraffic.model.Locality;
import org.hadooptraffic.model.Op;
import org.hadooptraffic.model.Role;
import org.hadooptraffic.model.TrafficEvent;
import org.junit.Test;

public class TrafficAggregatorTest {
  private static TrafficEvent ev(String user, Op op, Role role, long bytes, long est) {
    return new TrafficEvent(user, op, role, Locality.LOCAL, "bp-1", 1L, bytes, est, 1L, "p");
  }

  private static TrafficKey key(String user, Op op, Role role) {
    return new TrafficKey(user, op, role, Locality.LOCAL);
  }

  @Test
  public void sumsEventsWithSameKey() {
    TrafficAggregator agg = new TrafficAggregator(10);
    agg.onEvent(ev("alice", Op.READ, Role.CLIENT, 100, 0));
    agg.onEvent(ev("alice", Op.READ, Role.CLIENT, 50, 0));

    TrafficCounters.Snapshot s = agg.snapshot().get(key("alice", Op.READ, Role.CLIENT));
    assertEquals(150L, s.getBytes());
    assertEquals(2L, s.getOps());
    assertEquals(0L, s.getEstimatedBytes());
  }

  @Test
  public void separatesKeysByEveryDimension() {
    TrafficAggregator agg = new TrafficAggregator(10);
    agg.onEvent(ev("alice", Op.WRITE, Role.CLIENT, 10, 0));
    agg.onEvent(ev("alice", Op.WRITE, Role.PIPELINE, 20, 0));
    agg.onEvent(ev("bob", Op.WRITE, Role.CLIENT, 30, 0));
    agg.onEvent(new TrafficEvent("alice", Op.WRITE, Role.CLIENT, Locality.REMOTE,
        "bp-1", 1L, 40, 0, 1L, "p"));

    Map<TrafficKey, TrafficCounters.Snapshot> snap = agg.snapshot();
    assertEquals(4, snap.size());
    assertEquals(20L, snap.get(key("alice", Op.WRITE, Role.PIPELINE)).getBytes());
    assertEquals(40L, snap.get(new TrafficKey("alice", Op.WRITE, Role.CLIENT,
        Locality.REMOTE)).getBytes());
  }

  @Test
  public void countsEstimatedBytes() {
    TrafficAggregator agg = new TrafficAggregator(10);
    agg.onEvent(ev("alice", Op.SCR_FDS, Role.CLIENT, 0, 4096));

    TrafficCounters.Snapshot s = agg.snapshot().get(key("alice", Op.SCR_FDS, Role.CLIENT));
    assertEquals(0L, s.getBytes());
    assertEquals(4096L, s.getEstimatedBytes());
    assertEquals(1L, s.getOps());
  }

  @Test
  public void capsDistinctUsers() {
    TrafficAggregator agg = new TrafficAggregator(2);
    agg.onEvent(ev("alice", Op.READ, Role.CLIENT, 1, 0));
    agg.onEvent(ev("bob", Op.READ, Role.CLIENT, 1, 0));
    agg.onEvent(ev("carol", Op.READ, Role.CLIENT, 7, 0));
    agg.onEvent(ev("alice", Op.READ, Role.CLIENT, 1, 0));

    Map<TrafficKey, TrafficCounters.Snapshot> snap = agg.snapshot();
    assertFalse(snap.containsKey(key("carol", Op.READ, Role.CLIENT)));
    assertEquals(7L, snap.get(key(TrafficAggregator.OTHER_USER, Op.READ, Role.CLIENT)).getBytes());
    assertEquals(2L, snap.get(key("alice", Op.READ, Role.CLIENT)).getBytes());
  }

  @Test
  public void snapshotIsDetachedFromLaterEvents() {
    TrafficAggregator agg = new TrafficAggregator(10);
    agg.onEvent(ev("alice", Op.READ, Role.CLIENT, 1, 0));
    Map<TrafficKey, TrafficCounters.Snapshot> before = agg.snapshot();
    agg.onEvent(ev("alice", Op.READ, Role.CLIENT, 1, 0));

    assertEquals(1L, before.get(key("alice", Op.READ, Role.CLIENT)).getBytes());
    assertTrue(agg.snapshot().get(key("alice", Op.READ, Role.CLIENT)).getBytes() == 2L);
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsNonPositiveCap() {
    new TrafficAggregator(0);
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q test -Dtest=TrafficAggregatorTest`
Expected: compilation FAILURE (`cannot find symbol: class TrafficAggregator`).

- [ ] **Step 3: Implement**

`TrafficKey.java`:

```java
package org.hadooptraffic.aggregate;

import java.util.Objects;
import org.hadooptraffic.model.Locality;
import org.hadooptraffic.model.Op;
import org.hadooptraffic.model.Role;

/** Aggregation key: one time series per distinct value. */
public final class TrafficKey {
  private final String user;
  private final Op op;
  private final Role role;
  private final Locality locality;

  public TrafficKey(String user, Op op, Role role, Locality locality) {
    this.user = Objects.requireNonNull(user, "user");
    this.op = Objects.requireNonNull(op, "op");
    this.role = Objects.requireNonNull(role, "role");
    this.locality = Objects.requireNonNull(locality, "locality");
  }

  public String getUser() { return user; }
  public Op getOp() { return op; }
  public Role getRole() { return role; }
  public Locality getLocality() { return locality; }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TrafficKey)) {
      return false;
    }
    TrafficKey k = (TrafficKey) o;
    return user.equals(k.user) && op == k.op && role == k.role && locality == k.locality;
  }

  @Override
  public int hashCode() {
    return Objects.hash(user, op, role, locality);
  }

  @Override
  public String toString() {
    return user + "/" + op.label() + "/" + role.label() + "/" + locality.label();
  }
}
```

`TrafficCounters.java`:

```java
package org.hadooptraffic.aggregate;

import java.util.concurrent.atomic.LongAdder;

/** Cumulative counters for one key. Thread-safe. */
public final class TrafficCounters {
  private final LongAdder bytes = new LongAdder();
  private final LongAdder estimatedBytes = new LongAdder();
  private final LongAdder ops = new LongAdder();

  void record(long b, long estimated) {
    if (b > 0) {
      bytes.add(b);
    }
    if (estimated > 0) {
      estimatedBytes.add(estimated);
    }
    ops.increment();
  }

  public Snapshot snapshot() {
    return new Snapshot(bytes.sum(), estimatedBytes.sum(), ops.sum());
  }

  /** Point-in-time copy of the counters. */
  public static final class Snapshot {
    private final long bytes;
    private final long estimatedBytes;
    private final long ops;

    public Snapshot(long bytes, long estimatedBytes, long ops) {
      this.bytes = bytes;
      this.estimatedBytes = estimatedBytes;
      this.ops = ops;
    }

    public long getBytes() { return bytes; }
    public long getEstimatedBytes() { return estimatedBytes; }
    public long getOps() { return ops; }
  }
}
```

`TrafficAggregator.java`:

```java
package org.hadooptraffic.aggregate;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.hadooptraffic.model.TrafficEvent;
import org.hadooptraffic.sink.TrafficSink;

/** Sink that keeps cumulative counters per (user, op, role, locality). */
public final class TrafficAggregator implements TrafficSink {
  public static final String OTHER_USER = "__other__";

  private final int maxUsers;
  private final ConcurrentHashMap<TrafficKey, TrafficCounters> counters =
      new ConcurrentHashMap<>();
  private final Set<String> users = ConcurrentHashMap.newKeySet();

  public TrafficAggregator(int maxUsers) {
    if (maxUsers < 1) {
      throw new IllegalArgumentException("maxUsers must be >= 1: " + maxUsers);
    }
    this.maxUsers = maxUsers;
  }

  @Override
  public void onEvent(TrafficEvent e) {
    TrafficKey key = new TrafficKey(admitUser(e.getUser()), e.getOp(), e.getRole(),
        e.getLocality());
    counters.computeIfAbsent(key, k -> new TrafficCounters())
        .record(e.getBytes(), e.getEstimatedBytes());
  }

  /** Returns the user itself while under the cap, otherwise {@link #OTHER_USER}. */
  private String admitUser(String user) {
    if (users.contains(user)) {
      return user;
    }
    synchronized (users) {
      if (users.contains(user)) {
        return user;
      }
      if (users.size() >= maxUsers) {
        return OTHER_USER;
      }
      users.add(user);
      return user;
    }
  }

  public Map<TrafficKey, TrafficCounters.Snapshot> snapshot() {
    Map<TrafficKey, TrafficCounters.Snapshot> out = new HashMap<>();
    for (Map.Entry<TrafficKey, TrafficCounters> e : counters.entrySet()) {
      out.put(e.getKey(), e.getValue().snapshot());
    }
    return Collections.unmodifiableMap(out);
  }

  /** Distinct users currently tracked (excluding {@link #OTHER_USER}). */
  public Set<String> trackedUsers() {
    return Collections.unmodifiableSet(new HashSet<>(users));
  }
}
```

- [ ] **Step 4: Run tests in both profiles**

Run: `mvn -q test -Dtest=TrafficAggregatorTest && mvn -q -P hadoop-3.1.1 test -Dtest=TrafficAggregatorTest`
Expected: exit code 0.

- [ ] **Step 5: Checkpoint** — show `git status`; suggested message `feat: per-user traffic aggregator with cardinality cap`.

---

### Task 3: metrics2 source and registrar

**Files:**
- Create: `src/main/java/org/hadooptraffic/metrics/MetricName.java`, `UserTrafficMetricsSource.java`, `MetricsRegistrar.java`
- Test: `src/test/java/org/hadooptraffic/metrics/UserTrafficMetricsSourceTest.java`

**Interfaces:**
- Consumes: `TrafficAggregator.snapshot()`, `TrafficKey`, `TrafficCounters.Snapshot` (Task 2).
- Produces: `UserTrafficMetricsSource(TrafficAggregator)`, `static Map<String, Long> UserTrafficMetricsSource.metricValues(Map<TrafficKey, TrafficCounters.Snapshot>)`, `static String UserTrafficMetricsSource.attributeName(String counter, TrafficKey)`, constants `RECORD = "UserTraffic"`; `MetricsRegistrar.init(TrafficAggregator)`, `MetricsRegistrar.registerOnce()`, constant `MetricsRegistrar.SOURCE_NAME = "UserTraffic"`.

Note: `MetricsRecordImpl` is package-private in hadoop-common, so the unit test checks the pure `metricValues` function; JMX publication is verified by `DataTransferProtocolIT` (Task 6).

- [ ] **Step 1: Write the failing test**

```java
package org.hadooptraffic.metrics;

import static org.junit.Assert.assertEquals;

import java.util.Map;
import org.hadooptraffic.aggregate.TrafficAggregator;
import org.hadooptraffic.aggregate.TrafficKey;
import org.hadooptraffic.model.Locality;
import org.hadooptraffic.model.Op;
import org.hadooptraffic.model.Role;
import org.hadooptraffic.model.TrafficEvent;
import org.junit.Test;

public class UserTrafficMetricsSourceTest {
  @Test
  public void attributeNameEncodesAllDimensions() {
    TrafficKey k = new TrafficKey("alice", Op.WEBHDFS_READ, Role.CLIENT, Locality.REMOTE);
    assertEquals("Bytes;user=alice;op=webhdfs_read;role=client;locality=remote",
        UserTrafficMetricsSource.attributeName("Bytes", k));
  }

  @Test
  public void exposesCounterTripletPerKey() {
    TrafficAggregator agg = new TrafficAggregator(10);
    agg.onEvent(new TrafficEvent("alice", Op.WRITE, Role.CLIENT, Locality.REMOTE,
        "bp", 1L, 100L, 0L, 1L, "p"));
    agg.onEvent(new TrafficEvent("bob", Op.SCR_FDS, Role.CLIENT, Locality.LOCAL,
        "bp", 2L, 0L, 512L, 1L, "p"));

    Map<String, Long> m = UserTrafficMetricsSource.metricValues(agg.snapshot());

    assertEquals(6, m.size());
    assertEquals(Long.valueOf(100L),
        m.get("Bytes;user=alice;op=write;role=client;locality=remote"));
    assertEquals(Long.valueOf(1L),
        m.get("Ops;user=alice;op=write;role=client;locality=remote"));
    assertEquals(Long.valueOf(0L),
        m.get("EstimatedBytes;user=alice;op=write;role=client;locality=remote"));
    assertEquals(Long.valueOf(512L),
        m.get("EstimatedBytes;user=bob;op=scr_fds;role=client;locality=local"));
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q test -Dtest=UserTrafficMetricsSourceTest`
Expected: compilation FAILURE (`cannot find symbol: class UserTrafficMetricsSource`).

- [ ] **Step 3: Implement**

`MetricName.java`:

```java
package org.hadooptraffic.metrics;

import org.apache.hadoop.metrics2.MetricsInfo;

/**
 * Plain MetricsInfo. Deliberately not using Interns.info(): its cache is capped (~2000 names)
 * and logs overflow warnings with high-cardinality per-user names.
 */
final class MetricName implements MetricsInfo {
  private final String name;
  private final String description;

  MetricName(String name, String description) {
    this.name = name;
    this.description = description;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public String description() {
    return description;
  }

  @Override
  public String toString() {
    return name;
  }
}
```

`UserTrafficMetricsSource.java`:

```java
package org.hadooptraffic.metrics;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.hadooptraffic.aggregate.TrafficAggregator;
import org.hadooptraffic.aggregate.TrafficCounters;
import org.hadooptraffic.aggregate.TrafficKey;

/**
 * Publishes aggregator counters as one metrics2 record. Dimensions are encoded into metric
 * names ("Bytes;user=u;op=o;role=r;locality=l") because metrics2 flattens multi-record
 * sources into suffixed JMX attributes and Ambari Metrics ignores tags.
 */
public final class UserTrafficMetricsSource implements MetricsSource {
  public static final String RECORD = "UserTraffic";
  private static final MetricName RECORD_INFO =
      new MetricName(RECORD, "Per-user DataNode traffic");

  private final TrafficAggregator aggregator;

  public UserTrafficMetricsSource(TrafficAggregator aggregator) {
    this.aggregator = aggregator;
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder rb = collector.addRecord(RECORD_INFO).setContext("dfs");
    for (Map.Entry<String, Long> e : metricValues(aggregator.snapshot()).entrySet()) {
      rb.addCounter(new MetricName(e.getKey(), describe(e.getKey())), e.getValue());
    }
  }

  /** Flattens a snapshot into metric name -> value. */
  public static Map<String, Long> metricValues(Map<TrafficKey, TrafficCounters.Snapshot> snap) {
    Map<String, Long> out = new LinkedHashMap<>();
    for (Map.Entry<TrafficKey, TrafficCounters.Snapshot> e : snap.entrySet()) {
      TrafficKey k = e.getKey();
      TrafficCounters.Snapshot s = e.getValue();
      out.put(attributeName("Bytes", k), s.getBytes());
      out.put(attributeName("EstimatedBytes", k), s.getEstimatedBytes());
      out.put(attributeName("Ops", k), s.getOps());
    }
    return out;
  }

  public static String attributeName(String counter, TrafficKey k) {
    return counter + ";user=" + k.getUser() + ";op=" + k.getOp().label()
        + ";role=" + k.getRole().label() + ";locality=" + k.getLocality().label();
  }

  private static String describe(String name) {
    if (name.startsWith("EstimatedBytes;")) {
      return "Estimated bytes (short-circuit reads, not observed on the wire)";
    }
    if (name.startsWith("Bytes;")) {
      return "Payload bytes transferred";
    }
    return "Operations";
  }
}
```

`MetricsRegistrar.java`:

```java
package org.hadooptraffic.metrics;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.hadooptraffic.aggregate.TrafficAggregator;

/** Registers the metrics source once per JVM, after the DataNode metrics system is up. */
public final class MetricsRegistrar {
  public static final String SOURCE_NAME = "UserTraffic";

  private static volatile TrafficAggregator aggregator;
  private static final AtomicBoolean REGISTERED = new AtomicBoolean();

  private MetricsRegistrar() {
  }

  /** Called by the agent at install time. */
  public static void init(TrafficAggregator agg) {
    aggregator = agg;
  }

  /** Called on exit of DataNode.startDataNode; later calls are no-ops. */
  public static void registerOnce() {
    TrafficAggregator agg = aggregator;
    if (agg == null || !REGISTERED.compareAndSet(false, true)) {
      return;
    }
    DefaultMetricsSystem.instance().register(SOURCE_NAME, "Per-user DataNode traffic",
        new UserTrafficMetricsSource(agg));
  }
}
```

- [ ] **Step 4: Run tests in both profiles**

Run: `mvn -q test -Dtest=UserTrafficMetricsSourceTest && mvn -q -P hadoop-3.1.1 test -Dtest=UserTrafficMetricsSourceTest`
Expected: exit code 0.

- [ ] **Step 5: Checkpoint** — show `git status`; suggested message `feat: metrics2 source for per-user traffic`.

---

### Task 4: Resolvers (user, role, locality, argument lookup)

**Files:**
- Create: `src/main/java/org/hadooptraffic/context/Args.java`, `UserResolver.java`, `RoleResolver.java`, `LocalityResolver.java`
- Create (test helper): `src/test/java/org/hadooptraffic/it/TestTokens.java`
- Test: `src/test/java/org/hadooptraffic/context/UserResolverTest.java`, `RoleResolverTest.java`, `LocalityResolverTest.java`, `ArgsTest.java`

**Interfaces:**
- Consumes: `Op`, `Role`, `Locality` (Task 1).
- Produces:
  `Args.first(Object[] args, Class<T> type) : T` (null if absent);
  `UserResolver.UNKNOWN = "__unknown__"`, `UserResolver.userOf(Token<?>) : String`;
  `RoleResolver.WEBHDFS_CLIENT_PREFIX = "DFSClient_WEBHDFS_"`, `RoleResolver.resolve(Op op, String clientName, BlockConstructionStage stage, int pipelineSize, int targetCount) : Role`;
  `LocalityResolver.of(Peer) : Locality`, `LocalityResolver.of(SocketAddress) : Locality`, `LocalityResolver.ofHost(String ip) : Locality`;
  test helper `TestTokens.blockToken(String user, boolean proto) : Token<BlockTokenIdentifier>`.

- [ ] **Step 1: Write the test helper and failing tests**

`src/test/java/org/hadooptraffic/it/TestTokens.java`:

```java
package org.hadooptraffic.it;

import java.util.EnumSet;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;

/** Builds real serialized block tokens for unit tests. */
public final class TestTokens {
  private TestTokens() {
  }

  public static Token<BlockTokenIdentifier> blockToken(String user, boolean proto) {
    BlockTokenIdentifier id = new BlockTokenIdentifier(user, "bp-1", 42L,
        EnumSet.of(BlockTokenIdentifier.AccessMode.READ), new StorageType[0], new String[0],
        proto);
    return new Token<>(id.getBytes(), new byte[0], id.getKind(), new Text(""));
  }
}
```

`UserResolverTest.java`:

```java
package org.hadooptraffic.context;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.hadooptraffic.it.TestTokens;
import org.junit.Test;

public class UserResolverTest {
  @Test
  public void readsUserFromLegacyToken() {
    assertEquals("alice", UserResolver.userOf(TestTokens.blockToken("alice", false)));
  }

  @Test
  public void readsUserFromProtobufToken() {
    assertEquals("alice", UserResolver.userOf(TestTokens.blockToken("alice", true)));
  }

  @Test
  public void nullTokenIsUnknown() {
    assertEquals(UserResolver.UNKNOWN, UserResolver.userOf(null));
  }

  @Test
  public void emptyIdentifierIsUnknown() {
    Token<BlockTokenIdentifier> t = new Token<>(new byte[0], new byte[0], new Text(""),
        new Text(""));
    assertEquals(UserResolver.UNKNOWN, UserResolver.userOf(t));
  }

  @Test
  public void garbageIdentifierDoesNotThrow() {
    Token<BlockTokenIdentifier> t = new Token<>(new byte[] {(byte) 0xFF, 1, 2}, new byte[0],
        new Text(""), new Text(""));
    assertNotNull(UserResolver.userOf(t));
  }
}
```

`RoleResolverTest.java`:

```java
package org.hadooptraffic.context;

import static org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage.PIPELINE_SETUP_CREATE;
import static org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage.TRANSFER_FINALIZED;
import static org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage.TRANSFER_RBW;
import static org.junit.Assert.assertEquals;

import org.hadooptraffic.model.Op;
import org.hadooptraffic.model.Role;
import org.junit.Test;

public class RoleResolverTest {
  private static final String CLIENT = "DFSClient_NONMAPREDUCE_1_1";
  private static final String WEB = "DFSClient_WEBHDFS_1_1";

  @Test
  public void readByClient() {
    assertEquals(Role.CLIENT, RoleResolver.resolve(Op.READ, CLIENT, null, 0, 0));
  }

  @Test
  public void readByWebHdfsProxy() {
    assertEquals(Role.WEBHDFS_PROXY, RoleResolver.resolve(Op.READ, WEB, null, 0, 0));
  }

  @Test
  public void emptyClientNameIsDatanode() {
    assertEquals(Role.DATANODE, RoleResolver.resolve(Op.READ, "", null, 0, 0));
    assertEquals(Role.DATANODE,
        RoleResolver.resolve(Op.WRITE, "", PIPELINE_SETUP_CREATE, 0, 0));
  }

  @Test
  public void pipelineHeadIsClient() {
    assertEquals(Role.CLIENT,
        RoleResolver.resolve(Op.WRITE, CLIENT, PIPELINE_SETUP_CREATE, 3, 2));
  }

  @Test
  public void pipelineHeadForWebHdfsIsProxy() {
    assertEquals(Role.WEBHDFS_PROXY,
        RoleResolver.resolve(Op.WRITE, WEB, PIPELINE_SETUP_CREATE, 3, 2));
  }

  @Test
  public void downstreamIsPipelineEvenForWebHdfs() {
    assertEquals(Role.PIPELINE,
        RoleResolver.resolve(Op.WRITE, CLIENT, PIPELINE_SETUP_CREATE, 3, 1));
    assertEquals(Role.PIPELINE,
        RoleResolver.resolve(Op.WRITE, CLIENT, PIPELINE_SETUP_CREATE, 3, 0));
    assertEquals(Role.PIPELINE,
        RoleResolver.resolve(Op.WRITE, WEB, PIPELINE_SETUP_CREATE, 3, 0));
  }

  @Test
  public void transferStagesAreDatanode() {
    assertEquals(Role.DATANODE, RoleResolver.resolve(Op.WRITE, CLIENT, TRANSFER_RBW, 0, 0));
    assertEquals(Role.DATANODE,
        RoleResolver.resolve(Op.WRITE, CLIENT, TRANSFER_FINALIZED, 0, 1));
  }

  @Test
  public void balancerAndTransferOpsAreDatanode() {
    assertEquals(Role.DATANODE, RoleResolver.resolve(Op.COPY, null, null, 0, 0));
    assertEquals(Role.DATANODE, RoleResolver.resolve(Op.REPLACE, null, null, 0, 0));
    assertEquals(Role.DATANODE, RoleResolver.resolve(Op.TRANSFER, CLIENT, null, 0, 1));
  }

  @Test
  public void shortCircuitWithoutClientNameIsClient() {
    assertEquals(Role.CLIENT, RoleResolver.resolve(Op.SCR_FDS, null, null, 0, 0));
  }
}
```

`LocalityResolverTest.java`:

```java
package org.hadooptraffic.context;

import static org.junit.Assert.assertEquals;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import org.apache.hadoop.hdfs.net.Peer;
import org.hadooptraffic.model.Locality;
import org.junit.Test;

public class LocalityResolverTest {
  @Test
  public void loopbackIsLocal() throws Exception {
    assertEquals(Locality.LOCAL,
        LocalityResolver.of(new InetSocketAddress(InetAddress.getLoopbackAddress(), 1)));
    assertEquals(Locality.LOCAL, LocalityResolver.ofHost("127.0.0.1"));
  }

  @Test
  public void documentationAddressIsRemote() throws Exception {
    assertEquals(Locality.REMOTE,
        LocalityResolver.of(new InetSocketAddress(InetAddress.getByName("192.0.2.1"), 1)));
    assertEquals(Locality.REMOTE, LocalityResolver.ofHost("192.0.2.1"));
  }

  @Test
  public void unresolvedOrMissingIsRemote() {
    assertEquals(Locality.REMOTE,
        LocalityResolver.of(InetSocketAddress.createUnresolved("example.invalid", 1)));
    assertEquals(Locality.REMOTE, LocalityResolver.of((java.net.SocketAddress) null));
    assertEquals(Locality.REMOTE, LocalityResolver.of((Peer) null));
    assertEquals(Locality.REMOTE, LocalityResolver.ofHost(null));
  }
}
```

`ArgsTest.java`:

```java
package org.hadooptraffic.context;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.junit.Test;

public class ArgsTest {
  @Test
  public void findsFirstInstanceByType() {
    DatanodeInfo[] targets = new DatanodeInfo[2];
    Object[] args = {"a", 3, null, targets, "b", 7};
    assertEquals("a", Args.first(args, String.class));
    assertEquals(Integer.valueOf(3), Args.first(args, Integer.class));
    assertSame(targets, Args.first(args, DatanodeInfo[].class));
  }

  @Test
  public void returnsNullWhenAbsent() {
    assertNull(Args.first(new Object[] {"a"}, Long.class));
    assertNull(Args.first(null, String.class));
  }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn -q test -Dtest='UserResolverTest,RoleResolverTest,LocalityResolverTest,ArgsTest'`
Expected: compilation FAILURE (`cannot find symbol: class UserResolver` etc.).

- [ ] **Step 3: Implement**

`Args.java`:

```java
package org.hadooptraffic.context;

/** Locates intercepted method arguments by type, tolerating signature drift across versions. */
public final class Args {
  private Args() {
  }

  public static <T> T first(Object[] args, Class<T> type) {
    if (args == null) {
      return null;
    }
    for (Object a : args) {
      if (type.isInstance(a)) {
        return type.cast(a);
      }
    }
    return null;
  }
}
```

`UserResolver.java`:

```java
package org.hadooptraffic.context;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.security.token.Token;

/** Extracts the effective user from a block access token. */
public final class UserResolver {
  public static final String UNKNOWN = "__unknown__";

  private UserResolver() {
  }

  public static String userOf(Token<?> token) {
    if (token == null) {
      return UNKNOWN;
    }
    byte[] ident = token.getIdentifier();
    if (ident == null || ident.length == 0) {
      return UNKNOWN;
    }
    try {
      BlockTokenIdentifier id = new BlockTokenIdentifier();
      // readFields handles both legacy and protobuf encodings; it needs a mark-capable stream.
      id.readFields(new DataInputStream(new ByteArrayInputStream(ident)));
      String user = id.getUserId();
      return user == null || user.isEmpty() ? UNKNOWN : user;
    } catch (IOException | RuntimeException e) {
      return UNKNOWN;
    }
  }
}
```

`RoleResolver.java`:

```java
package org.hadooptraffic.context;

import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.hadooptraffic.model.Op;
import org.hadooptraffic.model.Role;

/** Classifies a DataTransferProtocol operation by who initiated it. */
public final class RoleResolver {
  /** Client-name prefix of DFSClients created by the DN WebHDFS handler (see WebHdfsHooks). */
  public static final String WEBHDFS_CLIENT_PREFIX = "DFSClient_WEBHDFS_";

  private RoleResolver() {
  }

  /**
   * @param pipelineSize writeBlock pipelineSize argument (full pipeline length as sent by the
   *     client; 0 for DN-initiated transfers)
   * @param targetCount number of downstream targets received by this DN
   */
  public static Role resolve(Op op, String clientName, BlockConstructionStage stage,
      int pipelineSize, int targetCount) {
    switch (op) {
      case COPY:
      case REPLACE:
      case TRANSFER:
        return Role.DATANODE;
      default:
        break;
    }
    if (clientName != null && clientName.isEmpty()) {
      return Role.DATANODE;
    }
    boolean webhdfs = clientName != null && clientName.startsWith(WEBHDFS_CLIENT_PREFIX);
    if (op == Op.WRITE) {
      if (stage == BlockConstructionStage.TRANSFER_RBW
          || stage == BlockConstructionStage.TRANSFER_FINALIZED) {
        return Role.DATANODE;
      }
      // The sender strips itself from the target list, so position 0 is the pipeline head.
      int position = pipelineSize - 1 - targetCount;
      if (position > 0) {
        return Role.PIPELINE;
      }
    }
    return webhdfs ? Role.WEBHDFS_PROXY : Role.CLIENT;
  }
}
```

`LocalityResolver.java`:

```java
package org.hadooptraffic.context;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.net.NetUtils;
import org.hadooptraffic.model.Locality;

/** Decides whether a peer runs on this host. */
public final class LocalityResolver {
  private LocalityResolver() {
  }

  public static Locality of(Peer peer) {
    if (peer == null) {
      return Locality.REMOTE;
    }
    try {
      return peer.getDomainSocket() != null || peer.isLocal() ? Locality.LOCAL : Locality.REMOTE;
    } catch (RuntimeException e) {
      return Locality.REMOTE;
    }
  }

  public static Locality of(SocketAddress address) {
    if (address instanceof InetSocketAddress) {
      return of(((InetSocketAddress) address).getAddress());
    }
    return Locality.REMOTE;
  }

  /** Locality of a literal IP address (as in DatanodeInfo.getIpAddr()). */
  public static Locality ofHost(String ip) {
    if (ip == null || ip.isEmpty()) {
      return Locality.REMOTE;
    }
    try {
      return of(InetAddress.getByName(ip));
    } catch (Exception e) {
      return Locality.REMOTE;
    }
  }

  private static Locality of(InetAddress a) {
    if (a == null) {
      return Locality.REMOTE;
    }
    // NetUtils caches results per address.
    return a.isLoopbackAddress() || NetUtils.isLocalAddress(a) ? Locality.LOCAL : Locality.REMOTE;
  }
}
```

- [ ] **Step 4: Run tests in both profiles**

Run: `mvn -q test -Dtest='UserResolverTest,RoleResolverTest,LocalityResolverTest,ArgsTest' && mvn -q -P hadoop-3.1.1 test -Dtest='UserResolverTest,RoleResolverTest,LocalityResolverTest,ArgsTest'`
Expected: exit code 0.

- [ ] **Step 5: Checkpoint** — show `git status`; suggested message `feat: user, role and locality resolvers`.

---

### Task 5: Operation context and DataXceiver hooks

**Files:**
- Create: `src/main/java/org/hadooptraffic/context/OpContext.java`, `OpContexts.java`
- Create: `src/main/java/org/hadooptraffic/hooks/XceiverHooks.java`
- Test: `src/test/java/org/hadooptraffic/hooks/XceiverHooksTest.java`

**Interfaces:**
- Consumes: Task 1 model + `TrafficSinks`; Task 4 resolvers and `Args`; test helper `TestTokens`.
- Produces:
  `OpContext(String user, Op op, Role role, Locality locality, String blockPoolId, long blockId, String peer)`, `addBytes(long)`, `addEstimatedBytes(long)`, `bytes()`, `finish()` (emits once);
  `OpContexts.current()`, `OpContexts.begin(OpContext) : boolean`, `OpContexts.end(boolean started)`, `OpContexts.addBytes(long)`;
  `XceiverHooks.onXceiverEnter(String method, Object[] args, Peer peer) : boolean`,
  `XceiverHooks.onDataTransferEnter(ExtendedBlock b, DatanodeInfo[] targets) : boolean`.

- [ ] **Step 1: Write the failing test**

```java
package org.hadooptraffic.hooks;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.security.UserGroupInformation;
import org.hadooptraffic.context.OpContexts;
import org.hadooptraffic.it.TestTokens;
import org.hadooptraffic.model.Locality;
import org.hadooptraffic.model.Op;
import org.hadooptraffic.model.Role;
import org.hadooptraffic.model.TrafficEvent;
import org.hadooptraffic.sink.TrafficSinks;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class XceiverHooksTest {
  private final List<TrafficEvent> events = new ArrayList<>();

  @Before
  public void setUp() {
    TrafficSinks.clear();
    TrafficSinks.register(events::add);
  }

  @After
  public void tearDown() {
    TrafficSinks.clear();
  }

  private static ExtendedBlock block(long len) {
    return new ExtendedBlock("bp-1", 42L, len, 1L);
  }

  @Test
  public void readBlockAccumulatesPacketsAndEmitsOnce() {
    Object[] args = {block(100), TestTokens.blockToken("alice", false),
        "DFSClient_NONMAPREDUCE_1_1", 0L, 100L, true, null};

    boolean started = XceiverHooks.onXceiverEnter("readBlock", args, null);
    OpContexts.addBytes(60);
    OpContexts.addBytes(40);
    OpContexts.addBytes(-1);
    OpContexts.end(started);
    OpContexts.end(started);

    assertTrue(started);
    assertNull(OpContexts.current());
    assertEquals(1, events.size());
    TrafficEvent e = events.get(0);
    assertEquals("alice", e.getUser());
    assertEquals(Op.READ, e.getOp());
    assertEquals(Role.CLIENT, e.getRole());
    assertEquals(Locality.REMOTE, e.getLocality());
    assertEquals("bp-1", e.getBlockPoolId());
    assertEquals(42L, e.getBlockId());
    assertEquals(100L, e.getBytes());
  }

  @Test
  public void writeBlockUsesPipelinePosition() {
    Object[] args = {block(0), null, TestTokens.blockToken("bob", true),
        "DFSClient_NONMAPREDUCE_1_1", new DatanodeInfo[1], null, null,
        BlockConstructionStage.PIPELINE_SETUP_CREATE, 3, 0L, 0L, 0L};

    boolean started = XceiverHooks.onXceiverEnter("writeBlock", args, null);
    OpContexts.end(started);

    assertEquals(Role.PIPELINE, events.get(0).getRole());
    assertEquals("bob", events.get(0).getUser());
  }

  @Test
  public void shortCircuitRecordsEstimatedBytes() {
    Object[] args = {block(4096), TestTokens.blockToken("carol", false), null, 1, true};

    boolean started = XceiverHooks.onXceiverEnter("requestShortCircuitFds", args, null);
    OpContexts.end(started);

    TrafficEvent e = events.get(0);
    assertEquals(Op.SCR_FDS, e.getOp());
    assertEquals(0L, e.getBytes());
    assertEquals(4096L, e.getEstimatedBytes());
  }

  @Test
  public void dataTransferNestedInXceiverOpDoesNotStartNewContext() {
    Object[] args = {block(10), TestTokens.blockToken("dave", false), "DFSClient_x",
        new DatanodeInfo[0], null, null};
    boolean outer = XceiverHooks.onXceiverEnter("transferBlock", args, null);

    boolean inner = XceiverHooks.onDataTransferEnter(block(10), null);
    OpContexts.addBytes(10);
    OpContexts.end(inner);
    OpContexts.end(outer);

    assertFalse(inner);
    assertEquals(1, events.size());
    assertEquals(Op.TRANSFER, events.get(0).getOp());
    assertEquals(10L, events.get(0).getBytes());
  }

  @Test
  public void standaloneDataTransferIsAttributedToDatanodeUser() throws Exception {
    boolean started = XceiverHooks.onDataTransferEnter(block(10), null);
    OpContexts.addBytes(10);
    OpContexts.end(started);

    TrafficEvent e = events.get(0);
    assertEquals(UserGroupInformation.getCurrentUser().getShortUserName(), e.getUser());
    assertEquals(Op.TRANSFER, e.getOp());
    assertEquals(Role.DATANODE, e.getRole());
  }

  @Test
  public void unknownMethodIsIgnored() {
    assertFalse(XceiverHooks.onXceiverEnter("blockChecksum", new Object[0], null));
    assertTrue(events.isEmpty());
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q test -Dtest=XceiverHooksTest`
Expected: compilation FAILURE (`cannot find symbol: class XceiverHooks`).

- [ ] **Step 3: Implement**

`OpContext.java`:

```java
package org.hadooptraffic.context;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.hadooptraffic.model.Locality;
import org.hadooptraffic.model.Op;
import org.hadooptraffic.model.Role;
import org.hadooptraffic.model.TrafficEvent;
import org.hadooptraffic.sink.TrafficSinks;

/** Mutable state of one in-flight operation. */
public final class OpContext {
  private final String user;
  private final Op op;
  private final Role role;
  private final Locality locality;
  private final String blockPoolId;
  private final long blockId;
  private final String peer;
  private final long startNanos = System.nanoTime();
  // Atomics: WebHDFS contexts may be touched from more than one thread.
  private final AtomicLong bytes = new AtomicLong();
  private final AtomicLong estimatedBytes = new AtomicLong();
  private final AtomicBoolean finished = new AtomicBoolean();

  public OpContext(String user, Op op, Role role, Locality locality, String blockPoolId,
      long blockId, String peer) {
    this.user = user;
    this.op = op;
    this.role = role;
    this.locality = locality;
    this.blockPoolId = blockPoolId;
    this.blockId = blockId;
    this.peer = peer;
  }

  public void addBytes(long n) {
    if (n > 0) {
      bytes.addAndGet(n);
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

  /** Emits the event to all sinks; only the first call has an effect. */
  public void finish() {
    if (!finished.compareAndSet(false, true)) {
      return;
    }
    long durationMs = (System.nanoTime() - startNanos) / 1_000_000L;
    TrafficSinks.emit(new TrafficEvent(user, op, role, locality, blockPoolId, blockId,
        bytes.get(), estimatedBytes.get(), durationMs, peer));
  }
}
```

`OpContexts.java`:

```java
package org.hadooptraffic.context;

/** Thread-bound context of the DataXceiver / DataTransfer operation running on this thread. */
public final class OpContexts {
  private static final ThreadLocal<OpContext> CURRENT = new ThreadLocal<>();

  private OpContexts() {
  }

  public static OpContext current() {
    return CURRENT.get();
  }

  /**
   * Binds ctx to this thread unless an operation is already running (nested calls, e.g.
   * DataXceiver.transferBlock -> DataTransfer.run, keep accounting to the outer op).
   *
   * @return true if the caller started the context and must call {@link #end(boolean)}
   */
  public static boolean begin(OpContext ctx) {
    if (CURRENT.get() != null) {
      return false;
    }
    CURRENT.set(ctx);
    return true;
  }

  public static void end(boolean started) {
    if (!started) {
      return;
    }
    OpContext ctx = CURRENT.get();
    CURRENT.remove();
    if (ctx != null) {
      ctx.finish();
    }
  }

  public static void addBytes(long n) {
    OpContext ctx = CURRENT.get();
    if (ctx != null) {
      ctx.addBytes(n);
    }
  }
}
```

`XceiverHooks.java`:

```java
package org.hadooptraffic.hooks;

import java.io.IOException;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.hadooptraffic.context.Args;
import org.hadooptraffic.context.LocalityResolver;
import org.hadooptraffic.context.OpContext;
import org.hadooptraffic.context.OpContexts;
import org.hadooptraffic.context.RoleResolver;
import org.hadooptraffic.context.UserResolver;
import org.hadooptraffic.model.Locality;
import org.hadooptraffic.model.Op;
import org.hadooptraffic.model.Role;

/** Logic behind the DataTransferProtocol advice; kept out of advice for testability. */
public final class XceiverHooks {
  private XceiverHooks() {
  }

  /** Entry of a DataXceiver op. @return true if a context was started. */
  public static boolean onXceiverEnter(String method, Object[] args, Peer peer) {
    Op op = Op.fromXceiverMethod(method);
    if (op == null) {
      return false;
    }
    Token<?> token = Args.first(args, Token.class);
    ExtendedBlock block = Args.first(args, ExtendedBlock.class);
    // clientName is the first String only for read/write/transfer (replaceBlock's is delHint).
    String clientName =
        op == Op.READ || op == Op.WRITE || op == Op.TRANSFER ? Args.first(args, String.class)
            : null;
    BlockConstructionStage stage = Args.first(args, BlockConstructionStage.class);
    DatanodeInfo[] targets = Args.first(args, DatanodeInfo[].class);
    Integer pipelineSize = op == Op.WRITE ? Args.first(args, Integer.class) : null;

    Role role = RoleResolver.resolve(op, clientName, stage,
        pipelineSize == null ? 0 : pipelineSize, targets == null ? 0 : targets.length);
    OpContext ctx = new OpContext(UserResolver.userOf(token), op, role,
        LocalityResolver.of(peer),
        block == null ? null : block.getBlockPoolId(),
        block == null ? -1L : block.getBlockId(),
        peer == null ? null : peer.getRemoteAddressString());
    if (op == Op.SCR_FDS && block != null) {
      // Short-circuit reads bypass the DN; block length is the best available estimate.
      ctx.addEstimatedBytes(block.getNumBytes());
    }
    return OpContexts.begin(ctx);
  }

  /** Entry of DataNode$DataTransfer.run (DN-initiated replication / pipeline recovery). */
  public static boolean onDataTransferEnter(ExtendedBlock block, DatanodeInfo[] targets) {
    if (OpContexts.current() != null) {
      return false;
    }
    DatanodeInfo target = targets != null && targets.length > 0 ? targets[0] : null;
    Locality locality = target == null ? Locality.REMOTE : LocalityResolver.ofHost(target.getIpAddr());
    OpContext ctx = new OpContext(dataNodeUser(), Op.TRANSFER, Role.DATANODE, locality,
        block == null ? null : block.getBlockPoolId(),
        block == null ? -1L : block.getBlockId(),
        target == null ? null : target.getXferAddr());
    return OpContexts.begin(ctx);
  }

  /** DataTransfer generates its block token for the DN's own user, so we report that user. */
  private static String dataNodeUser() {
    try {
      return UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (IOException e) {
      return UserResolver.UNKNOWN;
    }
  }
}
```

- [ ] **Step 4: Run tests in both profiles**

Run: `mvn -q test -Dtest=XceiverHooksTest && mvn -q -P hadoop-3.1.1 test -Dtest=XceiverHooksTest`
Expected: exit code 0.

- [ ] **Step 5: Checkpoint** — show `git status`; suggested message `feat: operation context and DataXceiver hooks`.

---

### Task 6: Agent bootstrap, DataTransferProtocol advice, MiniDFSCluster IT

**Files:**
- Create: `src/main/java/org/hadooptraffic/agent/AgentConfig.java`, `AgentLog.java`, `AgentLogListener.java`, `TrafficAgent.java`, `Instrumentations.java`
- Create: `src/main/java/org/hadooptraffic/advice/XceiverOpAdvice.java`, `SendPacketAdvice.java`, `ReceivePacketAdvice.java`, `DataTransferAdvice.java`, `StartDataNodeAdvice.java`
- Create (test support): `src/test/java/org/hadooptraffic/it/TestAgent.java`, `Await.java`, `TrafficAssert.java`, `ClusterTestBase.java`
- Test: `src/test/java/org/hadooptraffic/agent/AgentConfigTest.java`, `src/test/java/org/hadooptraffic/it/DataTransferProtocolIT.java`

**Interfaces:**
- Consumes: everything from Tasks 1–5; `MetricsRegistrar.init/registerOnce` (Task 3).
- Produces: `TrafficAgent.premain/agentmain(String, Instrumentation)`, `TrafficAgent.install(String, Instrumentation)`, `TrafficAgent.aggregator() : TrafficAggregator`;
  `AgentConfig.parse(String) : AgentConfig`, `maxUsers()`, `DEFAULT_MAX_USERS = 2000`;
  `AgentLog.info(String)`, `AgentLog.warn(String, Throwable)`;
  `Instrumentations.builder() : AgentBuilder` (Task 7 extends it), `Instrumentations.advice(Class<?>, ElementMatcher<? super MethodDescription>) : AgentBuilder.Transformer`;
  test support: `TestAgent.install()`, `Await.until(BooleanSupplier, long timeoutMs)`, `TrafficAssert.bytes(String user, Op, Role)`, `TrafficAssert.ops(...)`, `TrafficAssert.dump()`, `ClusterTestBase` (`cluster`, `conf`, `DIR`, `FILE_LEN`, `as(String, PrivilegedExceptionAction<T>)`).

- [ ] **Step 1: Write the failing unit test for `AgentConfig`**

```java
package org.hadooptraffic.agent;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class AgentConfigTest {
  @Test
  public void defaultsWhenNoArgs() {
    assertEquals(AgentConfig.DEFAULT_MAX_USERS, AgentConfig.parse(null).maxUsers());
    assertEquals(AgentConfig.DEFAULT_MAX_USERS, AgentConfig.parse("  ").maxUsers());
  }

  @Test
  public void parsesMaxUsers() {
    assertEquals(5, AgentConfig.parse("maxUsers=5").maxUsers());
    assertEquals(7, AgentConfig.parse(" maxUsers = 7 ,").maxUsers());
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsUnknownKey() {
    AgentConfig.parse("foo=1");
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsNonPositiveMaxUsers() {
    AgentConfig.parse("maxUsers=0");
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsMissingValue() {
    AgentConfig.parse("maxUsers");
  }
}
```

- [ ] **Step 2: Write the failing integration test and test support**

`src/test/java/org/hadooptraffic/it/TestAgent.java`:

```java
package org.hadooptraffic.it;

import net.bytebuddy.agent.ByteBuddyAgent;
import org.hadooptraffic.agent.TrafficAgent;

/** Installs the agent into the test JVM (idempotent). */
public final class TestAgent {
  private TestAgent() {
  }

  public static void install() {
    TrafficAgent.install("maxUsers=100", ByteBuddyAgent.install());
  }
}
```

`src/test/java/org/hadooptraffic/it/Await.java`:

```java
package org.hadooptraffic.it;

import java.util.function.BooleanSupplier;

/** Polls a condition: DN ops finish asynchronously after the client call returns. */
public final class Await {
  private Await() {
  }

  public static void until(BooleanSupplier condition, long timeoutMs)
      throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (!condition.getAsBoolean()) {
      if (System.currentTimeMillis() > deadline) {
        throw new AssertionError("Condition not met in " + timeoutMs + " ms. Counters:\n"
            + TrafficAssert.dump());
      }
      Thread.sleep(100);
    }
  }
}
```

`src/test/java/org/hadooptraffic/it/TrafficAssert.java`:

```java
package org.hadooptraffic.it;

import java.util.Map;
import java.util.TreeMap;
import org.hadooptraffic.agent.TrafficAgent;
import org.hadooptraffic.aggregate.TrafficCounters;
import org.hadooptraffic.aggregate.TrafficKey;
import org.hadooptraffic.model.Op;
import org.hadooptraffic.model.Role;

/** Reads the agent's aggregator, summing over localities. */
public final class TrafficAssert {
  private TrafficAssert() {
  }

  public static long bytes(String user, Op op, Role role) {
    long sum = 0;
    for (Map.Entry<TrafficKey, TrafficCounters.Snapshot> e : snapshot().entrySet()) {
      if (matches(e.getKey(), user, op, role)) {
        sum += e.getValue().getBytes();
      }
    }
    return sum;
  }

  public static long ops(String user, Op op, Role role) {
    long sum = 0;
    for (Map.Entry<TrafficKey, TrafficCounters.Snapshot> e : snapshot().entrySet()) {
      if (matches(e.getKey(), user, op, role)) {
        sum += e.getValue().getOps();
      }
    }
    return sum;
  }

  public static String dump() {
    Map<String, String> sorted = new TreeMap<>();
    for (Map.Entry<TrafficKey, TrafficCounters.Snapshot> e : snapshot().entrySet()) {
      TrafficCounters.Snapshot s = e.getValue();
      sorted.put(e.getKey().toString(), "bytes=" + s.getBytes() + " est="
          + s.getEstimatedBytes() + " ops=" + s.getOps());
    }
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, String> e : sorted.entrySet()) {
      sb.append("  ").append(e.getKey()).append(": ").append(e.getValue()).append('\n');
    }
    return sb.toString();
  }

  private static Map<TrafficKey, TrafficCounters.Snapshot> snapshot() {
    return TrafficAgent.aggregator().snapshot();
  }

  private static boolean matches(TrafficKey k, String user, Op op, Role role) {
    return k.getUser().equals(user) && k.getOp() == op && k.getRole() == role;
  }
}
```

`src/test/java/org/hadooptraffic/it/ClusterTestBase.java`:

```java
package org.hadooptraffic.it;

import java.security.PrivilegedExceptionAction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/** 3-DN MiniDFSCluster with block tokens (simple auth) and the agent installed. */
public abstract class ClusterTestBase {
  protected static final int BLOCK_SIZE = 1024 * 1024;
  protected static final int FILE_LEN = 2 * BLOCK_SIZE + 12345;
  protected static final long AWAIT_MS = 20_000;
  protected static final Path DIR = new Path("/traffic");

  protected static Configuration conf;
  protected static MiniDFSCluster cluster;

  @BeforeClass
  public static void startCluster() throws Exception {
    TestAgent.install();
    conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setBoolean(DFSConfigKeys.DFS_WEBHDFS_ENABLED_KEY, true);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();
    FileSystem fs = cluster.getFileSystem();
    fs.mkdirs(DIR);
    fs.setPermission(DIR, new FsPermission((short) 0777));
  }

  @AfterClass
  public static void stopCluster() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  protected static <T> T as(String user, PrivilegedExceptionAction<T> action) throws Exception {
    return UserGroupInformation.createRemoteUser(user).doAs(action);
  }
}
```

`src/test/java/org/hadooptraffic/it/DataTransferProtocolIT.java`:

```java
package org.hadooptraffic.it;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.lang.management.ManagementFactory;
import java.util.Set;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.hadooptraffic.model.Op;
import org.hadooptraffic.model.Role;
import org.junit.Test;

public class DataTransferProtocolIT extends ClusterTestBase {

  @Test
  public void clientWriteAndPipelineForwardingAreSeparated() throws Exception {
    String user = "alice-write";
    Path p = new Path(DIR, "write-file");
    as(user, () -> {
      try (FileSystem fs = FileSystem.newInstance(cluster.getURI(), conf)) {
        DFSTestUtil.createFile(fs, p, FILE_LEN, (short) 3, 1L);
      }
      return null;
    });

    Await.until(() -> TrafficAssert.bytes(user, Op.WRITE, Role.CLIENT) == FILE_LEN
        && TrafficAssert.bytes(user, Op.WRITE, Role.PIPELINE) == 2L * FILE_LEN, AWAIT_MS);
    assertEquals(3L, TrafficAssert.ops(user, Op.WRITE, Role.CLIENT));
  }

  @Test
  public void readIsAttributedToReader() throws Exception {
    Path p = new Path(DIR, "read-file");
    as("writer-for-read", () -> {
      try (FileSystem fs = FileSystem.newInstance(cluster.getURI(), conf)) {
        DFSTestUtil.createFile(fs, p, FILE_LEN, (short) 1, 2L);
      }
      return null;
    });
    String reader = "bob-read";
    as(reader, () -> {
      try (FileSystem fs = FileSystem.newInstance(cluster.getURI(), conf)) {
        DFSTestUtil.readFile(fs, p);
      }
      return null;
    });

    Await.until(() -> TrafficAssert.bytes(reader, Op.READ, Role.CLIENT) == FILE_LEN, AWAIT_MS);
    assertEquals(0L, TrafficAssert.bytes(reader, Op.WRITE, Role.CLIENT));
  }

  @Test
  public void namenodeTriggeredReplicationIsDatanodeRole() throws Exception {
    String dnUser = UserGroupInformation.getCurrentUser().getShortUserName();
    long transferBefore = TrafficAssert.bytes(dnUser, Op.TRANSFER, Role.DATANODE);
    long writeBefore = TrafficAssert.bytes(dnUser, Op.WRITE, Role.DATANODE);
    Path p = new Path(DIR, "replicate-file");
    as("carol-repl", () -> {
      try (FileSystem fs = FileSystem.newInstance(cluster.getURI(), conf)) {
        DFSTestUtil.createFile(fs, p, FILE_LEN, (short) 1, 3L);
      }
      return null;
    });

    FileSystem admin = cluster.getFileSystem();
    admin.setReplication(p, (short) 2);
    DFSTestUtil.waitReplication(admin, p, (short) 2);

    Await.until(() ->
        TrafficAssert.bytes(dnUser, Op.TRANSFER, Role.DATANODE) - transferBefore == FILE_LEN
        && TrafficAssert.bytes(dnUser, Op.WRITE, Role.DATANODE) - writeBefore == FILE_LEN,
        AWAIT_MS);
  }

  @Test
  public void countersArePublishedViaJmx() throws Exception {
    String user = "dave-jmx";
    Path p = new Path(DIR, "jmx-file");
    as(user, () -> {
      try (FileSystem fs = FileSystem.newInstance(cluster.getURI(), conf)) {
        DFSTestUtil.createFile(fs, p, FILE_LEN, (short) 1, 4L);
      }
      return null;
    });
    Await.until(() -> TrafficAssert.bytes(user, Op.WRITE, Role.CLIENT) == FILE_LEN, AWAIT_MS);

    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    Set<ObjectName> names = mbs.queryNames(new ObjectName("Hadoop:service=*,name=UserTraffic"),
        null);
    assertEquals(1, names.size());
    ObjectName name = names.iterator().next();
    String prefix = "Bytes;user=" + user + ";op=write;role=client;locality=";
    String[] attr = new String[1];
    // The metrics2 JMX cache refreshes after its TTL (~10 s), so poll.
    Await.until(() -> {
      try {
        for (MBeanAttributeInfo a : mbs.getMBeanInfo(name).getAttributes()) {
          if (a.getName().startsWith(prefix)) {
            attr[0] = a.getName();
            return true;
          }
        }
        return false;
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }, 30_000);
    assertNotNull(attr[0]);
    assertEquals((long) FILE_LEN, ((Number) mbs.getAttribute(name, attr[0])).longValue());
  }
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `mvn -q test -Dtest='AgentConfigTest,DataTransferProtocolIT'`
Expected: compilation FAILURE (`cannot find symbol: class AgentConfig` / `TrafficAgent`).

- [ ] **Step 4: Implement the agent core**

`AgentConfig.java`:

```java
package org.hadooptraffic.agent;

/** Parsed -javaagent option string: comma-separated key=value pairs. */
public final class AgentConfig {
  public static final int DEFAULT_MAX_USERS = 2000;

  private final int maxUsers;

  private AgentConfig(int maxUsers) {
    this.maxUsers = maxUsers;
  }

  public int maxUsers() {
    return maxUsers;
  }

  public static AgentConfig parse(String args) {
    int maxUsers = DEFAULT_MAX_USERS;
    if (args != null) {
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
        if ("maxUsers".equals(key)) {
          maxUsers = Integer.parseInt(value);
          if (maxUsers < 1) {
            throw new IllegalArgumentException("maxUsers must be >= 1: " + maxUsers);
          }
        } else {
          throw new IllegalArgumentException("Unknown agent option: " + key);
        }
      }
    }
    return new AgentConfig(maxUsers);
  }
}
```

`AgentLog.java`:

```java
package org.hadooptraffic.agent;

/** Logs via slf4j when available (always on a DataNode), otherwise to stderr. */
public final class AgentLog {
  private static final String NAME = "org.hadooptraffic.agent";

  private AgentLog() {
  }

  public static void info(String msg) {
    try {
      org.slf4j.LoggerFactory.getLogger(NAME).info(msg);
    } catch (Throwable t) {
      System.err.println("[hdfs-traffic-agent] INFO " + msg);
    }
  }

  public static void warn(String msg, Throwable error) {
    try {
      org.slf4j.LoggerFactory.getLogger(NAME).warn(msg, error);
    } catch (Throwable t) {
      System.err.println("[hdfs-traffic-agent] WARN " + msg);
      if (error != null) {
        error.printStackTrace();
      }
    }
  }
}
```

`AgentLogListener.java`:

```java
package org.hadooptraffic.agent;

import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.utility.JavaModule;

/** Reports instrumented classes and instrumentation failures to the DataNode log. */
final class AgentLogListener extends AgentBuilder.Listener.Adapter {
  @Override
  public void onTransformation(TypeDescription type, ClassLoader loader, JavaModule module,
      boolean loaded, DynamicType dynamicType) {
    AgentLog.info("hdfs-traffic-agent: instrumented " + type.getName());
  }

  @Override
  public void onError(String typeName, ClassLoader loader, JavaModule module, boolean loaded,
      Throwable error) {
    AgentLog.warn("hdfs-traffic-agent: failed to instrument " + typeName
        + "; traffic of this class is not accounted", error);
  }
}
```

`Instrumentations.java`:

```java
package org.hadooptraffic.agent;

import static net.bytebuddy.matcher.ElementMatchers.isPublic;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.nameStartsWith;
import static net.bytebuddy.matcher.ElementMatchers.namedOneOf;
import static net.bytebuddy.matcher.ElementMatchers.returns;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.matcher.ElementMatcher;
import org.hadooptraffic.advice.DataTransferAdvice;
import org.hadooptraffic.advice.ReceivePacketAdvice;
import org.hadooptraffic.advice.SendPacketAdvice;
import org.hadooptraffic.advice.StartDataNodeAdvice;
import org.hadooptraffic.advice.XceiverOpAdvice;

/** Declares which Hadoop methods get which advice. */
final class Instrumentations {
  static final String DN = "org.apache.hadoop.hdfs.server.datanode.";

  private Instrumentations() {
  }

  static AgentBuilder builder() {
    return new AgentBuilder.Default()
        .disableClassFormatChanges()
        .with(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION)
        .with(new AgentLogListener())
        .ignore(nameStartsWith("net.bytebuddy.").or(nameStartsWith("org.hadooptraffic.")))
        .type(named(DN + "DataXceiver"))
        .transform(advice(XceiverOpAdvice.class, isPublic().and(namedOneOf("readBlock",
            "writeBlock", "transferBlock", "copyBlock", "replaceBlock",
            "requestShortCircuitFds"))))
        .type(named(DN + "BlockSender"))
        .transform(advice(SendPacketAdvice.class,
            named("sendPacket").and(returns(int.class))))
        .type(named(DN + "BlockReceiver"))
        .transform(advice(ReceivePacketAdvice.class,
            named("receivePacket").and(takesArguments(0)).and(returns(int.class))))
        .type(named(DN + "DataNode$DataTransfer"))
        .transform(advice(DataTransferAdvice.class, named("run").and(takesArguments(0))))
        .type(named(DN + "DataNode"))
        .transform(advice(StartDataNodeAdvice.class, named("startDataNode")));
  }

  /**
   * Applies advice to matching methods; if nothing matches (signature drift in a Hadoop
   * version), logs a WARN and leaves the class untouched so only this hook is disabled.
   */
  static AgentBuilder.Transformer advice(Class<?> adviceClass,
      ElementMatcher<? super MethodDescription> methods) {
    return (builder, type, classLoader, module, protectionDomain) -> {
      if (type.getDeclaredMethods().filter(methods).isEmpty()) {
        AgentLog.warn("hdfs-traffic-agent: no method matched " + adviceClass.getSimpleName()
            + " in " + type.getName() + "; this hook is disabled", null);
        return builder;
      }
      return builder.visit(Advice.to(adviceClass).on(methods));
    };
  }
}
```

`TrafficAgent.java`:

```java
package org.hadooptraffic.agent;

import java.lang.instrument.Instrumentation;
import org.hadooptraffic.aggregate.TrafficAggregator;
import org.hadooptraffic.metrics.MetricsRegistrar;
import org.hadooptraffic.sink.TrafficSinks;

/** Entry point: -javaagent:hdfs-traffic-agent.jar[=maxUsers=N]. */
public final class TrafficAgent {
  private static volatile TrafficAggregator aggregator;
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
      TrafficAggregator agg = new TrafficAggregator(config.maxUsers());
      TrafficSinks.register(agg);
      MetricsRegistrar.init(agg);
      aggregator = agg;
      Instrumentations.builder().installOn(inst);
      AgentLog.info("hdfs-traffic-agent installed (maxUsers=" + config.maxUsers() + ")");
    } catch (Throwable t) {
      AgentLog.warn("hdfs-traffic-agent disabled: " + t, t);
    }
  }

  /** The aggregator, or null if the agent is not installed. */
  public static TrafficAggregator aggregator() {
    return aggregator;
  }
}
```

- [ ] **Step 5: Implement the advice classes**

`XceiverOpAdvice.java`:

```java
package org.hadooptraffic.advice;

import net.bytebuddy.asm.Advice;
import org.apache.hadoop.hdfs.net.Peer;
import org.hadooptraffic.context.OpContexts;
import org.hadooptraffic.hooks.XceiverHooks;

/** Inlined into DataXceiver ops. Keep logic in XceiverHooks. */
public final class XceiverOpAdvice {
  private XceiverOpAdvice() {
  }

  @Advice.OnMethodEnter(suppress = Throwable.class)
  public static boolean enter(@Advice.Origin("#m") String method,
      @Advice.AllArguments Object[] args, @Advice.FieldValue("peer") Peer peer) {
    return XceiverHooks.onXceiverEnter(method, args, peer);
  }

  @Advice.OnMethodExit(onThrowable = Throwable.class, suppress = Throwable.class)
  public static void exit(@Advice.Enter boolean started) {
    OpContexts.end(started);
  }
}
```

`SendPacketAdvice.java`:

```java
package org.hadooptraffic.advice;

import net.bytebuddy.asm.Advice;
import org.hadooptraffic.context.OpContexts;

/** BlockSender.sendPacket returns the payload length of the packet sent. */
public final class SendPacketAdvice {
  private SendPacketAdvice() {
  }

  @Advice.OnMethodExit(suppress = Throwable.class)
  public static void exit(@Advice.Return int dataLen) {
    OpContexts.addBytes(dataLen);
  }
}
```

`ReceivePacketAdvice.java`:

```java
package org.hadooptraffic.advice;

import net.bytebuddy.asm.Advice;
import org.hadooptraffic.context.OpContexts;

/** BlockReceiver.receivePacket returns payload length, 0 for heartbeats, -1 for the last packet. */
public final class ReceivePacketAdvice {
  private ReceivePacketAdvice() {
  }

  @Advice.OnMethodExit(suppress = Throwable.class)
  public static void exit(@Advice.Return int len) {
    OpContexts.addBytes(len);
  }
}
```

`DataTransferAdvice.java`:

```java
package org.hadooptraffic.advice;

import net.bytebuddy.asm.Advice;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.hadooptraffic.context.OpContexts;
import org.hadooptraffic.hooks.XceiverHooks;

/** Inlined into DataNode$DataTransfer.run. */
public final class DataTransferAdvice {
  private DataTransferAdvice() {
  }

  @Advice.OnMethodEnter(suppress = Throwable.class)
  public static boolean enter(@Advice.FieldValue("b") ExtendedBlock block,
      @Advice.FieldValue("targets") DatanodeInfo[] targets) {
    return XceiverHooks.onDataTransferEnter(block, targets);
  }

  @Advice.OnMethodExit(onThrowable = Throwable.class, suppress = Throwable.class)
  public static void exit(@Advice.Enter boolean started) {
    OpContexts.end(started);
  }
}
```

`StartDataNodeAdvice.java`:

```java
package org.hadooptraffic.advice;

import net.bytebuddy.asm.Advice;
import org.hadooptraffic.metrics.MetricsRegistrar;

/** Registers the metrics source once the DataNode (and its metrics system) has started. */
public final class StartDataNodeAdvice {
  private StartDataNodeAdvice() {
  }

  @Advice.OnMethodExit(suppress = Throwable.class)
  public static void exit() {
    MetricsRegistrar.registerOnce();
  }
}
```

- [ ] **Step 6: Run the unit test and the IT in both profiles**

Run: `mvn -q test -Dtest='AgentConfigTest,DataTransferProtocolIT' && mvn -q -P hadoop-3.1.1 test -Dtest='AgentConfigTest,DataTransferProtocolIT'`
Expected: exit code 0. If `DataTransferProtocolIT` fails, read `target/surefire-reports/org.hadooptraffic.it.DataTransferProtocolIT-output.txt`: it contains `hdfs-traffic-agent: instrumented ...` lines (all five DN classes must appear) and the counter dump from `Await`. Debug with superpowers:systematic-debugging — do not loosen assertions.

- [ ] **Step 7: Checkpoint** — show `git status`; suggested message `feat: ByteBuddy agent for DataTransferProtocol traffic`.

---

### Task 7: WebHDFS accounting

**Files:**
- Create: `src/main/java/org/hadooptraffic/hooks/WebHdfsHooks.java`
- Create: `src/main/java/org/hadooptraffic/advice/WebHdfsRequestAdvice.java`, `NewDfsClientAdvice.java`, `WebHdfsReadStreamCtorAdvice.java`, `WebHdfsReadStreamCloseAdvice.java`, `HdfsWriterCtorAdvice.java`, `HdfsWriterContentAdvice.java`, `HdfsWriterReleaseAdvice.java`
- Modify: `src/main/java/org/hadooptraffic/agent/Instrumentations.java` (append WebHDFS rules to `builder()`)
- Test: `src/test/java/org/hadooptraffic/hooks/WebHdfsHooksTest.java`, `src/test/java/org/hadooptraffic/it/WebHdfsIT.java`

**Interfaces:**
- Consumes: `OpContext` (Task 5), `LocalityResolver`, `UserResolver` (Task 4), `Instrumentations.advice` (Task 6), `ClusterTestBase`, `Await`, `TrafficAssert` (Task 6).
- Produces: `WebHdfsHooks.TASK_ID = "WEBHDFS"`, `onRequestEnter(String method, UserGroupInformation ugi, ChannelHandlerContext ctx) : boolean`, `onRequestExit(boolean)`, `tagClientConf(Configuration) : Configuration`, `bindPending(Object owner)`, `addBytes(Object owner, long n)`, `finish(Object owner)`, `finishRead(ChunkedStream stream)`.

- [ ] **Step 1: Write the failing unit test**

```java
package org.hadooptraffic.hooks;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.hadooptraffic.model.Locality;
import org.hadooptraffic.model.Op;
import org.hadooptraffic.model.Role;
import org.hadooptraffic.model.TrafficEvent;
import org.hadooptraffic.sink.TrafficSinks;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class WebHdfsHooksTest {
  private final List<TrafficEvent> events = new ArrayList<>();
  private final UserGroupInformation erin = UserGroupInformation.createRemoteUser("erin");

  @Before
  public void setUp() {
    TrafficSinks.clear();
    TrafficSinks.register(events::add);
  }

  @After
  public void tearDown() {
    TrafficSinks.clear();
  }

  @Test
  public void boundWriterAccumulatesAndFinishesOnce() {
    Object writer = new Object();
    boolean started = WebHdfsHooks.onRequestEnter("onCreate", erin, null);
    WebHdfsHooks.bindPending(writer);
    WebHdfsHooks.onRequestExit(started);

    WebHdfsHooks.addBytes(writer, 100);
    WebHdfsHooks.addBytes(writer, 23);
    WebHdfsHooks.finish(writer);
    WebHdfsHooks.finish(writer);

    assertEquals(1, events.size());
    TrafficEvent e = events.get(0);
    assertEquals("erin", e.getUser());
    assertEquals(Op.WEBHDFS_CREATE, e.getOp());
    assertEquals(Role.CLIENT, e.getRole());
    assertEquals(Locality.REMOTE, e.getLocality());
    assertEquals(123L, e.getBytes());
    assertEquals(-1L, e.getBlockId());
  }

  @Test
  public void requestWithoutBoundStreamEmitsNothing() {
    boolean started = WebHdfsHooks.onRequestEnter("onOpen", erin, null);
    WebHdfsHooks.onRequestExit(started);

    assertTrue(started);
    assertTrue(events.isEmpty());
  }

  @Test
  public void unknownMethodIsIgnored() {
    Object owner = new Object();
    boolean started = WebHdfsHooks.onRequestEnter("onGetFileChecksum", erin, null);
    WebHdfsHooks.bindPending(owner);
    WebHdfsHooks.onRequestExit(started);
    WebHdfsHooks.finish(owner);

    assertTrue(events.isEmpty());
  }

  @Test
  public void tagsClientConfOnlyInsideRequest() {
    Configuration conf = new Configuration(false);
    assertSame(conf, WebHdfsHooks.tagClientConf(conf));

    boolean started = WebHdfsHooks.onRequestEnter("onOpen", erin, null);
    Configuration tagged = WebHdfsHooks.tagClientConf(conf);
    WebHdfsHooks.onRequestExit(started);

    assertNotSame(conf, tagged);
    assertEquals(WebHdfsHooks.TASK_ID, tagged.get("mapreduce.task.attempt.id"));
    assertNull(conf.get("mapreduce.task.attempt.id"));
  }
}
```

- [ ] **Step 2: Write the failing IT**

`src/test/java/org/hadooptraffic/it/WebHdfsIT.java`:

```java
package org.hadooptraffic.it;

import static org.junit.Assert.assertEquals;

import java.net.URI;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.net.NetUtils;
import org.hadooptraffic.model.Op;
import org.hadooptraffic.model.Role;
import org.junit.Test;

public class WebHdfsIT extends ClusterTestBase {

  private static URI webUri() throws Exception {
    return new URI("webhdfs://"
        + NetUtils.getHostPortString(cluster.getNameNode().getHttpAddress()));
  }

  @Test
  public void writeIsCountedOnHttpAndInternalLegs() throws Exception {
    String user = "erin-web-write";
    Path p = new Path(DIR, "web-write");
    as(user, () -> {
      try (FileSystem web = FileSystem.newInstance(webUri(), conf)) {
        DFSTestUtil.createFile(web, p, FILE_LEN, (short) 3, 5L);
      }
      return null;
    });

    Await.until(() -> TrafficAssert.bytes(user, Op.WEBHDFS_CREATE, Role.CLIENT) == FILE_LEN
        && TrafficAssert.bytes(user, Op.WRITE, Role.WEBHDFS_PROXY) == FILE_LEN
        && TrafficAssert.bytes(user, Op.WRITE, Role.PIPELINE) == 2L * FILE_LEN, AWAIT_MS);
    assertEquals(0L, TrafficAssert.bytes(user, Op.WRITE, Role.CLIENT));
  }

  @Test
  public void readIsCountedOnHttpAndInternalLegs() throws Exception {
    Path p = new Path(DIR, "web-read");
    as("writer-for-web-read", () -> {
      try (FileSystem fs = FileSystem.newInstance(cluster.getURI(), conf)) {
        DFSTestUtil.createFile(fs, p, FILE_LEN, (short) 1, 6L);
      }
      return null;
    });
    String user = "frank-web-read";
    as(user, () -> {
      try (FileSystem web = FileSystem.newInstance(webUri(), conf)) {
        DFSTestUtil.readFile(web, p);
      }
      return null;
    });

    Await.until(() -> TrafficAssert.bytes(user, Op.WEBHDFS_READ, Role.CLIENT) == FILE_LEN
        && TrafficAssert.bytes(user, Op.READ, Role.WEBHDFS_PROXY) == FILE_LEN, AWAIT_MS);
    assertEquals(0L, TrafficAssert.bytes(user, Op.READ, Role.CLIENT));
  }
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `mvn -q test -Dtest='WebHdfsHooksTest,WebHdfsIT'`
Expected: compilation FAILURE (`cannot find symbol: class WebHdfsHooks`).

- [ ] **Step 4: Implement `WebHdfsHooks`**

```java
package org.hadooptraffic.hooks;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedStream;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.hadooptraffic.context.LocalityResolver;
import org.hadooptraffic.context.OpContext;
import org.hadooptraffic.context.UserResolver;
import org.hadooptraffic.model.Op;
import org.hadooptraffic.model.Role;

/**
 * WebHDFS HTTP-leg accounting. A request handler method (onOpen/onCreate/onAppend) opens a
 * pending context on its thread; the read stream / HdfsWriter created inside it is bound to
 * that context and carries it until the transfer ends.
 */
public final class WebHdfsHooks {
  /** Task id injected into the DN-embedded DFSClient: client name becomes DFSClient_WEBHDFS_*. */
  public static final String TASK_ID = "WEBHDFS";
  private static final String TASK_ID_KEY = "mapreduce.task.attempt.id";

  private static final ThreadLocal<OpContext> PENDING = new ThreadLocal<>();
  // Weak keys: a stream that is never closed must not leak its context.
  private static final Map<Object, OpContext> BOUND =
      Collections.synchronizedMap(new WeakHashMap<>());

  private WebHdfsHooks() {
  }

  public static boolean onRequestEnter(String method, UserGroupInformation ugi,
      ChannelHandlerContext ctx) {
    Op op = Op.fromWebHdfsMethod(method);
    if (op == null) {
      return false;
    }
    String user = ugi == null ? UserResolver.UNKNOWN : ugi.getShortUserName();
    SocketAddress remote = ctx == null ? null : ctx.channel().remoteAddress();
    PENDING.set(new OpContext(user, op, Role.CLIENT, LocalityResolver.of(remote), null, -1L,
        remote == null ? null : remote.toString()));
    return true;
  }

  /** Drops the pending context; if nothing was bound, no data was transferred. */
  public static void onRequestExit(boolean started) {
    if (started) {
      PENDING.remove();
    }
  }

  /** Copies conf with the WebHDFS task id when called inside a request; otherwise returns it. */
  public static Configuration tagClientConf(Configuration conf) {
    if (conf == null || PENDING.get() == null) {
      return conf;
    }
    Configuration copy = new Configuration(conf);
    copy.set(TASK_ID_KEY, TASK_ID);
    return copy;
  }

  public static void bindPending(Object owner) {
    OpContext ctx = PENDING.get();
    if (ctx != null && owner != null) {
      BOUND.put(owner, ctx);
    }
  }

  public static void addBytes(Object owner, long n) {
    if (n <= 0) {
      return;
    }
    OpContext ctx = BOUND.get(owner);
    if (ctx != null) {
      ctx.addBytes(n);
    }
  }

  public static void finish(Object owner) {
    OpContext ctx = BOUND.remove(owner);
    if (ctx != null) {
      ctx.finish();
    }
  }

  /** Read path: the byte count comes from the Netty stream itself. */
  public static void finishRead(ChunkedStream stream) {
    OpContext ctx = BOUND.remove(stream);
    if (ctx != null) {
      ctx.addBytes(stream.transferredBytes());
      ctx.finish();
    }
  }
}
```

- [ ] **Step 5: Implement the WebHDFS advice classes**

`WebHdfsRequestAdvice.java`:

```java
package org.hadooptraffic.advice;

import io.netty.channel.ChannelHandlerContext;
import net.bytebuddy.asm.Advice;
import org.apache.hadoop.security.UserGroupInformation;
import org.hadooptraffic.hooks.WebHdfsHooks;

/** Inlined into WebHdfsHandler.onOpen/onCreate/onAppend. */
public final class WebHdfsRequestAdvice {
  private WebHdfsRequestAdvice() {
  }

  @Advice.OnMethodEnter(suppress = Throwable.class)
  public static boolean enter(@Advice.Origin("#m") String method,
      @Advice.FieldValue("ugi") UserGroupInformation ugi,
      @Advice.Argument(0) ChannelHandlerContext ctx) {
    return WebHdfsHooks.onRequestEnter(method, ugi, ctx);
  }

  @Advice.OnMethodExit(onThrowable = Throwable.class, suppress = Throwable.class)
  public static void exit(@Advice.Enter boolean started) {
    WebHdfsHooks.onRequestExit(started);
  }
}
```

`NewDfsClientAdvice.java`:

```java
package org.hadooptraffic.advice;

import net.bytebuddy.asm.Advice;
import org.apache.hadoop.conf.Configuration;
import org.hadooptraffic.hooks.WebHdfsHooks;

/** Inlined into WebHdfsHandler.newDfsClient(String, Configuration): tags the client name. */
public final class NewDfsClientAdvice {
  private NewDfsClientAdvice() {
  }

  @Advice.OnMethodEnter(suppress = Throwable.class)
  public static void enter(@Advice.Argument(value = 1, readOnly = false) Configuration conf) {
    conf = WebHdfsHooks.tagClientConf(conf);
  }
}
```

`WebHdfsReadStreamCtorAdvice.java`:

```java
package org.hadooptraffic.advice;

import net.bytebuddy.asm.Advice;
import org.hadooptraffic.hooks.WebHdfsHooks;

/** Inlined into the constructor of WebHdfsHandler's anonymous ChunkedStream. */
public final class WebHdfsReadStreamCtorAdvice {
  private WebHdfsReadStreamCtorAdvice() {
  }

  @Advice.OnMethodExit(suppress = Throwable.class)
  public static void exit(@Advice.This Object self) {
    WebHdfsHooks.bindPending(self);
  }
}
```

`WebHdfsReadStreamCloseAdvice.java`:

```java
package org.hadooptraffic.advice;

import io.netty.handler.stream.ChunkedStream;
import net.bytebuddy.asm.Advice;
import org.hadooptraffic.hooks.WebHdfsHooks;

/** Inlined into close() of WebHdfsHandler's anonymous ChunkedStream. */
public final class WebHdfsReadStreamCloseAdvice {
  private WebHdfsReadStreamCloseAdvice() {
  }

  @Advice.OnMethodEnter(suppress = Throwable.class)
  public static void enter(@Advice.This ChunkedStream self) {
    WebHdfsHooks.finishRead(self);
  }
}
```

`HdfsWriterCtorAdvice.java`:

```java
package org.hadooptraffic.advice;

import net.bytebuddy.asm.Advice;
import org.hadooptraffic.hooks.WebHdfsHooks;

/** Inlined into the HdfsWriter constructor (created inside onCreate/onAppend). */
public final class HdfsWriterCtorAdvice {
  private HdfsWriterCtorAdvice() {
  }

  @Advice.OnMethodExit(suppress = Throwable.class)
  public static void exit(@Advice.This Object self) {
    WebHdfsHooks.bindPending(self);
  }
}
```

`HdfsWriterContentAdvice.java`:

```java
package org.hadooptraffic.advice;

import io.netty.handler.codec.http.HttpContent;
import net.bytebuddy.asm.Advice;
import org.hadooptraffic.hooks.WebHdfsHooks;

/** Inlined into HdfsWriter.channelRead0(ChannelHandlerContext, HttpContent). */
public final class HdfsWriterContentAdvice {
  private HdfsWriterContentAdvice() {
  }

  @Advice.OnMethodEnter(suppress = Throwable.class)
  public static void enter(@Advice.This Object self, @Advice.Argument(1) HttpContent chunk) {
    WebHdfsHooks.addBytes(self, chunk.content().readableBytes());
  }
}
```

`HdfsWriterReleaseAdvice.java`:

```java
package org.hadooptraffic.advice;

import net.bytebuddy.asm.Advice;
import org.hadooptraffic.hooks.WebHdfsHooks;

/** Inlined into HdfsWriter.releaseDfsResources (end of body, channel close or error). */
public final class HdfsWriterReleaseAdvice {
  private HdfsWriterReleaseAdvice() {
  }

  @Advice.OnMethodEnter(suppress = Throwable.class)
  public static void enter(@Advice.This Object self) {
    WebHdfsHooks.finish(self);
  }
}
```

- [ ] **Step 6: Register WebHDFS rules in `Instrumentations`**

Add imports:

```java
import static net.bytebuddy.matcher.ElementMatchers.hasSuperType;
import static net.bytebuddy.matcher.ElementMatchers.isConstructor;
import static net.bytebuddy.matcher.ElementMatchers.takesArgument;

import org.hadooptraffic.advice.HdfsWriterContentAdvice;
import org.hadooptraffic.advice.HdfsWriterCtorAdvice;
import org.hadooptraffic.advice.HdfsWriterReleaseAdvice;
import org.hadooptraffic.advice.NewDfsClientAdvice;
import org.hadooptraffic.advice.WebHdfsReadStreamCloseAdvice;
import org.hadooptraffic.advice.WebHdfsReadStreamCtorAdvice;
import org.hadooptraffic.advice.WebHdfsRequestAdvice;
```

Add the constant next to `DN`:

```java
  static final String WEB = DN + "web.webhdfs.";
```

Replace the final `.transform(advice(StartDataNodeAdvice.class, named("startDataNode")));` line of `builder()` with:

```java
        .transform(advice(StartDataNodeAdvice.class, named("startDataNode")))
        .type(named(WEB + "WebHdfsHandler"))
        .transform(advice(WebHdfsRequestAdvice.class,
            namedOneOf("onOpen", "onCreate", "onAppend").and(takesArguments(1))))
        .transform(advice(NewDfsClientAdvice.class,
            named("newDfsClient").and(takesArguments(2))))
        .type(nameStartsWith(WEB + "WebHdfsHandler$")
            .and(hasSuperType(named("io.netty.handler.stream.ChunkedStream"))))
        .transform(advice(WebHdfsReadStreamCtorAdvice.class, isConstructor()))
        .transform(advice(WebHdfsReadStreamCloseAdvice.class,
            named("close").and(takesArguments(0))))
        .type(named(WEB + "HdfsWriter"))
        .transform(advice(HdfsWriterCtorAdvice.class, isConstructor()))
        .transform(advice(HdfsWriterContentAdvice.class, named("channelRead0")
            .and(takesArgument(1, named("io.netty.handler.codec.http.HttpContent")))))
        .transform(advice(HdfsWriterReleaseAdvice.class, named("releaseDfsResources")));
```

- [ ] **Step 7: Run tests in both profiles**

Run: `mvn -q test -Dtest='WebHdfsHooksTest,WebHdfsIT,DataTransferProtocolIT' && mvn -q -P hadoop-3.1.1 test -Dtest='WebHdfsHooksTest,WebHdfsIT,DataTransferProtocolIT'`
Expected: exit code 0. `DataTransferProtocolIT` is re-run to prove the WebHDFS rules did not disturb the DataTransferProtocol rules. On failure check `target/surefire-reports/org.hadooptraffic.it.WebHdfsIT-output.txt` for `instrumented ...WebHdfsHandler`, `...WebHdfsHandler$2`, `...HdfsWriter`.

- [ ] **Step 8: Checkpoint** — show `git status`; suggested message `feat: WebHDFS HTTP-leg accounting and internal-leg tagging`.

---

### Task 8: Kerberos smoke IT

**Files:**
- Test: `src/test/java/org/hadooptraffic/it/KerberosTrafficIT.java`

**Interfaces:**
- Consumes: `TestAgent`, `Await`, `TrafficAssert` (Task 6); Hadoop test class `SaslDataTransferTestCase` (`initKdc` is its `@BeforeClass`, `createSecureConfig(String)`, `getUserPrincipal()`, `getUserKeyTab()`).
- Produces: nothing new.

- [ ] **Step 1: Write the test**

```java
package org.hadooptraffic.it;

import java.security.PrivilegedExceptionAction;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferTestCase;
import org.apache.hadoop.security.UserGroupInformation;
import org.hadooptraffic.model.Op;
import org.hadooptraffic.model.Role;
import org.junit.BeforeClass;
import org.junit.Test;

/** Kerberos + SASL data transfer + block tokens: the user still comes from the block token. */
public class KerberosTrafficIT extends SaslDataTransferTestCase {
  private static final int LEN = 1024 * 1024 + 123;

  @BeforeClass
  public static void installAgent() {
    TestAgent.install();
  }

  @Test
  public void kerberosUserIsTakenFromBlockToken() throws Exception {
    HdfsConfiguration conf = createSecureConfig("authentication");
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY, false);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    try {
      cluster.waitActive();
      UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
          getUserPrincipal(), getUserKeyTab());
      String user = ugi.getShortUserName();
      Path p = new Path("/kerberos-file");
      ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
        try (FileSystem fs = FileSystem.newInstance(cluster.getURI(), conf)) {
          DFSTestUtil.createFile(fs, p, LEN, (short) 3, 9L);
          DFSTestUtil.readFile(fs, p);
        }
        return null;
      });

      Await.until(() -> TrafficAssert.bytes(user, Op.WRITE, Role.CLIENT) == LEN
          && TrafficAssert.bytes(user, Op.WRITE, Role.PIPELINE) == 2L * LEN
          && TrafficAssert.bytes(user, Op.READ, Role.CLIENT) == LEN, 30_000);
    } finally {
      cluster.shutdown();
    }
  }
}
```

- [ ] **Step 2: Run in both profiles**

Run: `mvn -q test -Dtest=KerberosTrafficIT && mvn -q -P hadoop-3.1.1 test -Dtest=KerberosTrafficIT`
Expected: exit code 0. (This task adds a test over already-implemented behavior, so it is expected to pass first time; if it fails with `NoClassDefFoundError: org/bouncycastle/...`, compare the bouncycastle artifacts in the active profile with `grep -A3 bcprov ~/.m2/repository/org/apache/hadoop/hadoop-project/<version>/hadoop-project-<version>.pom` and align the profile dependency.)

- [ ] **Step 3: Checkpoint** — show `git status`; suggested message `test: Kerberos smoke test for block-token user attribution`.

---

### Task 9: Agent packaging and documentation

**Files:**
- Modify: `pom.xml` (add `maven-shade-plugin` to `<build><plugins>`)
- Create: `README.md`, `README.ru.md`

**Interfaces:**
- Consumes: `org.hadooptraffic.agent.TrafficAgent` (Task 6).
- Produces: `target/hdfs-traffic-agent-0.1.0-SNAPSHOT.jar` with `Premain-Class`/`Agent-Class`.

- [ ] **Step 1: Add the shade plugin to `pom.xml`** (inside `<build><plugins>`, after surefire)

```xml
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-shade-plugin</artifactId>
        <version>3.5.1</version>
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
```

- [ ] **Step 2: Build and verify the jar**

Run:
```bash
mvn -q -DskipTests package
unzip -p target/hdfs-traffic-agent-0.1.0-SNAPSHOT.jar META-INF/MANIFEST.MF | grep -E 'Premain-Class|Agent-Class|Can-Retransform'
unzip -l target/hdfs-traffic-agent-0.1.0-SNAPSHOT.jar | grep -c 'org/hadooptraffic/shaded/net/bytebuddy/'
unzip -l target/hdfs-traffic-agent-0.1.0-SNAPSHOT.jar | grep -c ' net/bytebuddy/'
```
Expected: the three manifest lines; a positive count of relocated classes; `0` unrelocated `net/bytebuddy/` entries.

- [ ] **Step 3: Smoke-test the agent without Hadoop on the classpath**

Run: `$JAVA_HOME/bin/java -javaagent:target/hdfs-traffic-agent-0.1.0-SNAPSHOT.jar=maxUsers=10 -version`
Expected: Java version banner, an `[hdfs-traffic-agent] ...` line on stderr (slf4j/Hadoop are absent, so the agent either installs with stderr logging or logs `disabled`), and **exit code 0** — the agent must never prevent JVM start.

Then: `$JAVA_HOME/bin/java -javaagent:target/hdfs-traffic-agent-0.1.0-SNAPSHOT.jar=bogus=1 -version`
Expected: exit code 0 with a `disabled: java.lang.IllegalArgumentException: Unknown agent option: bogus` message.

- [ ] **Step 4: Write `README.md`**

````markdown
# hdfs-traffic-agent

Java agent for HDFS DataNodes that accounts traffic per user and exposes it via
Hadoop metrics2 / JMX. Supported: Apache Hadoop 3.3.x, HDP 3.1 (Hadoop 3.1.1), Java 8.

## What is counted

| Dimension | Values |
|-----------|--------|
| `user`    | user from the block access token; `__unknown__` without a token; `__other__` above `maxUsers` |
| `op`      | `read`, `write`, `transfer`, `copy`, `replace`, `scr_fds`, `webhdfs_read`, `webhdfs_create`, `webhdfs_append` |
| `role`    | `client`, `pipeline` (replica forwarding), `datanode` (re-replication, balancer, recovery), `webhdfs_proxy` (internal leg of WebHDFS) |
| `locality`| `local`, `remote` |

Counters per series: `Bytes` (payload bytes, no checksums/headers), `EstimatedBytes`
(short-circuit reads: block length per descriptor request — the DataNode cannot see the
actual bytes), `Ops`. Counters are cumulative since DataNode start.

Client-facing traffic = all `role=client` series. `webhdfs_proxy` duplicates the
`webhdfs_*` HTTP traffic on the internal DataTransferProtocol leg; exclude it to avoid
double counting.

## Build

```bash
export JAVA_HOME=/path/to/jdk8   # a full JDK 8 (with lib/tools.jar)
mvn -P hadoop-3.3.6 verify
mvn -P hadoop-3.1.1 verify
```

The jar is `target/hdfs-traffic-agent-<version>.jar` (same jar for both Hadoop lines).

## Install

1. Copy the jar to every DataNode, e.g. `/opt/hdfs-traffic-agent/hdfs-traffic-agent.jar`.
2. Add to `hadoop-env.sh` (HDP: Ambari → HDFS → Configs → Advanced hadoop-env template):

   ```bash
   export HDFS_DATANODE_OPTS="$HDFS_DATANODE_OPTS -javaagent:/opt/hdfs-traffic-agent/hdfs-traffic-agent.jar=maxUsers=2000"
   ```

   `HADOOP_DATANODE_OPTS` (deprecated name) is honored by Hadoop 3 as well.
3. Restart DataNodes. The DataNode log shows `hdfs-traffic-agent installed` and
   `hdfs-traffic-agent: instrumented ...` lines. A `WARN ... hook is disabled` line means
   a method was not found in this Hadoop build; other hooks keep working.

Requires `dfs.block.access.token.enable=true` (always the case with Kerberos); without it
every user is `__unknown__`.

## Metrics

JMX bean `Hadoop:service=DataNode,name=UserTraffic`, attributes named
`<Counter>;user=<u>;op=<op>;role=<role>;locality=<loc>`, e.g.
`Bytes;user=alice;op=read;role=client;locality=remote`. Also available at
`http://<dn>:<http-port>/jmx?qry=Hadoop:service=DataNode,name=UserTraffic`.

Prometheus jmx_exporter rule:

```yaml
rules:
  - pattern: 'Hadoop<service=DataNode, name=UserTraffic><>(Bytes|EstimatedBytes|Ops);user=([^;]+);op=(\w+);role=(\w+);locality=(\w+)'
    name: hdfs_datanode_user_traffic_$1_total
    type: COUNTER
    labels:
      user: "$2"
      op: "$3"
      role: "$4"
      locality: "$5"
```

## Not covered

- Exact bytes of short-circuit reads (see `EstimatedBytes`).
- Block-to-file mapping (planned; events already carry block pool and block id).
````

- [ ] **Step 5: Write `README.ru.md`** (same content in Russian)

````markdown
# hdfs-traffic-agent

Java-агент для DataNode HDFS: считает трафик в разрезе пользователей и публикует
счётчики через Hadoop metrics2 / JMX. Поддерживаются Apache Hadoop 3.3.x и HDP 3.1
(Hadoop 3.1.1), Java 8.

## Что считается

| Разрез    | Значения |
|-----------|----------|
| `user`    | пользователь из block access token; `__unknown__`, если токена нет; `__other__` сверх лимита `maxUsers` |
| `op`      | `read`, `write`, `transfer`, `copy`, `replace`, `scr_fds`, `webhdfs_read`, `webhdfs_create`, `webhdfs_append` |
| `role`    | `client`, `pipeline` (пересылка реплик), `datanode` (перерепликация, балансировщик, восстановление), `webhdfs_proxy` (внутреннее плечо WebHDFS) |
| `locality`| `local`, `remote` |

Счётчики каждого ряда: `Bytes` (полезные данные, без checksum и заголовков),
`EstimatedBytes` (short-circuit чтения: длина блока на каждый запрос дескриптора —
сами байты DataNode не видит), `Ops`. Значения нарастают с момента старта DataNode.

Клиентский трафик — все ряды с `role=client`. Роль `webhdfs_proxy` дублирует
HTTP-трафик `webhdfs_*` на внутреннем плече DataTransferProtocol; исключайте её,
чтобы не считать дважды.

## Сборка

```bash
export JAVA_HOME=/path/to/jdk8   # полный JDK 8 (с lib/tools.jar)
mvn -P hadoop-3.3.6 verify
mvn -P hadoop-3.1.1 verify
```

Результат — `target/hdfs-traffic-agent-<version>.jar`, один jar для обеих версий Hadoop.

## Установка

1. Скопировать jar на каждую DataNode, например в
   `/opt/hdfs-traffic-agent/hdfs-traffic-agent.jar`.
2. Добавить в `hadoop-env.sh` (HDP: Ambari → HDFS → Configs → Advanced hadoop-env, шаблон):

   ```bash
   export HDFS_DATANODE_OPTS="$HDFS_DATANODE_OPTS -javaagent:/opt/hdfs-traffic-agent/hdfs-traffic-agent.jar=maxUsers=2000"
   ```

   Старое имя `HADOOP_DATANODE_OPTS` Hadoop 3 тоже понимает.
3. Перезапустить DataNode. В логе появятся строки `hdfs-traffic-agent installed` и
   `hdfs-traffic-agent: instrumented ...`. Строка `WARN ... hook is disabled` значит, что
   в этой сборке Hadoop не найден метод; остальные перехваты продолжают работать.

Нужен `dfs.block.access.token.enable=true` (с Kerberos он включён всегда); без него все
пользователи будут `__unknown__`.

## Метрики

JMX-бин `Hadoop:service=DataNode,name=UserTraffic`, атрибуты вида
`<Counter>;user=<u>;op=<op>;role=<role>;locality=<loc>`, например
`Bytes;user=alice;op=read;role=client;locality=remote`. Те же данные доступны по
`http://<dn>:<http-port>/jmx?qry=Hadoop:service=DataNode,name=UserTraffic`.

Правило для Prometheus jmx_exporter:

```yaml
rules:
  - pattern: 'Hadoop<service=DataNode, name=UserTraffic><>(Bytes|EstimatedBytes|Ops);user=([^;]+);op=(\w+);role=(\w+);locality=(\w+)'
    name: hdfs_datanode_user_traffic_$1_total
    type: COUNTER
    labels:
      user: "$2"
      op: "$3"
      role: "$4"
      locality: "$5"
```

## Что не покрыто

- Точный объём short-circuit чтений (см. `EstimatedBytes`).
- Сопоставление блоков с файлами (следующий шаг; события уже содержат block pool и block id).
````

- [ ] **Step 6: Full verification in both profiles**

Run:
```bash
mvn -P hadoop-3.3.6 verify
mvn -P hadoop-3.1.1 verify
```
Expected: `BUILD SUCCESS` twice; surefire summary `Tests run: <N>, Failures: 0, Errors: 0, Skipped: 0` where N covers OpTest, TrafficSinksTest, TrafficAggregatorTest, UserTrafficMetricsSourceTest, UserResolverTest, RoleResolverTest, LocalityResolverTest, ArgsTest, XceiverHooksTest, AgentConfigTest, WebHdfsHooksTest, DataTransferProtocolIT, WebHdfsIT, KerberosTrafficIT.

- [ ] **Step 7: Checkpoint** — show `git status` and `git diff --stat`; suggested message `build: shaded agent jar, README (EN/RU)`.

---

## Known limitations (carry into README if they change)

- Read bytes are BlockSender payload bytes: positional reads that are not checksum-chunk aligned can count up to one chunk (512 B by default) more than the client requested.
- Bytes of a write packet that fails mid-`receivePacket` are not counted (the method throws before returning its length).
- A single short-circuit descriptor request may serve many reads from the client cache; `EstimatedBytes` counts the block length once per request.
