# HDFS DataNode per-user traffic accounting — design

Date: 2026-11-06
Status: draft, awaiting review

## Goal

Account DataNode traffic per user and expose it via Hadoop metrics2 / JMX.
Dimensions: user, operation type, role (client / pipeline / datanode /
webhdfs_proxy), locality (local / remote). Metrics: bytes and operation count.

Step 2 (out of scope here) will map blocks to file names and may add new
sinks; the event model below keeps `blockPoolId`/`blockId` for that.

## Environment

- Apache Hadoop 3.3.x and HDP 3.1 (Hadoop 3.1.1), Java 8.
- Kerberos enabled, `dfs.block.access.token.enable=true` — the user is taken
  from `BlockTokenIdentifier.getUserId()`.

## 1. Architecture and build

- Standalone Maven project `hdfs-traffic-agent`, output: one shaded jar
  (ByteBuddy relocated to avoid classpath conflicts), bytecode target Java 8.
- Hadoop dependencies are `provided`; Maven profiles `hadoop-3.1.1` and
  `hadoop-3.3.6` compile and test against both lines.
- Deployment: `-javaagent:/path/hdfs-traffic-agent.jar=maxUsers=2000` in
  `HADOOP_DATANODE_OPTS` (HDP: Ambari → hadoop-env). No Hadoop jars are
  modified.

## 2. Interception (DataTransferProtocol)

Advice on `org.apache.hadoop.hdfs.server.datanode.DataXceiver` operations:
`readBlock`, `writeBlock`, `transferBlock`, `copyBlock`, `replaceBlock`,
`requestShortCircuitFds`. Arguments are captured with `@AllArguments` and
located by type (not index) to tolerate signature differences between 3.1
and 3.3.

On op entry an `OpContext` is put into a ThreadLocal:

- `user` — `BlockTokenIdentifier.getUserId()`; `__unknown__` if no token can
  be decoded.
- `op` — `read`, `write`, `transfer`, `copy`, `replace`, `scr_fds`.
- `role`:
  - `copyBlock` / `replaceBlock` / `transferBlock` → `datanode`;
  - empty `clientName` (any op) → `datanode` (DN-initiated transfer, EC reconstruction);
  - `writeBlock` with stage `TRANSFER_RBW` / `TRANSFER_FINALIZED` → `datanode`;
  - `writeBlock` pipeline position `pipelineSize - 1 - targets.length` > 0 → `pipeline`;
  - otherwise `clientName` starting with `DFSClient_WEBHDFS_` → `webhdfs_proxy`,
    else `client`.
- `locality` — `local` if the peer is a domain socket or its address belongs
  to a local interface, otherwise `remote`.
- `blockPoolId`, `blockId`, peer address, start time.

Bytes are added per packet to the current `OpContext` from the return values
of `BlockSender.sendPacket` and `BlockReceiver.receivePacket`, so aborted
transfers are counted too. Counted bytes are payload data bytes (no
checksums, no headers). The exact return-value semantics of these private
methods are verified per Hadoop version in the compatibility tests.

DN-initiated replication (NameNode command) runs in `DataNode$DataTransfer.run`
outside `DataXceiver`; it is intercepted as op `transfer`, role `datanode`,
user from its block token (usually the service user `hdfs`).

Short-circuit reads bypass the DataNode data path: only op `scr_fds` is
counted, plus `estimatedBytes` = block length (explicitly an estimate).

## 3. Event model and output

On op exit (including exceptional exit) a
`TrafficEvent{user, op, role, locality, bpid, blockId, bytes, estimatedBytes,
durationMs, peer}` is emitted to the `TrafficSink` interface. Step 1 has a
single implementation — the aggregator. Block-to-file mapping in step 2 is
another sink; interception does not change.

Aggregator: `ConcurrentHashMap<Key(user, op, role, locality), Counters>` with
`LongAdder` bytes / estimatedBytes / ops. Counters are cumulative since DN
start (suitable for Prometheus `rate()`).

metrics2 source `UserTraffic`, registered on exit of `DataNode.startDataNode`
(metrics system is initialised by then). JMX:
`Hadoop:service=DataNode,name=UserTraffic`, a single record whose metric names
encode the dimensions: `<Counter>.user.<u>.op.<op>.role.<role>.locality.<loc>`
with counters `Bytes`, `EstimatedBytes`, `Ops` (nntop precedent). Names avoid `=`, `:`
and spaces, which Hadoop's `/jmx` servlet silently drops (found on the Docker stand). Tags are not
used because metrics2 flattens multi-record sources into suffixed JMX
attributes (`Bytes.1`, …) and Ambari Metrics ignores tags.

Cardinality cap: once `maxUsers` distinct users are tracked, new users are
aggregated into `__other__`.

## 4. WebHDFS

DataNode serves WebHDFS via Netty `WebHdfsHandler`, which internally uses a
`DFSClient` running as the end user. Traffic therefore has two legs:

1. HTTP leg: external client ↔ DataNode.
2. Internal leg: internal `DFSClient` ↔ `DataXceiver` (same or another DN).

HTTP leg:

- Advice on `WebHdfsHandler.onOpen` / `onCreate` / `onAppend`: user from
  `UserGroupInformation.getCurrentUser()` (handler already runs in `doAs`),
  locality from `ctx.channel().remoteAddress()`; context in a ThreadLocal for
  the duration of the call.
- Read: the handler's anonymous `ChunkedStream` subclass
  (`WebHdfsHandler$N`) is bound to the context through a weak map at
  construction; on its `close()` the byte count is taken from
  `ChunkedStream.transferredBytes()` (no Netty-wide instrumentation).
- Write: constructor of `HdfsWriter` bound the same way; bytes summed from
  `HttpContent.content().readableBytes()` in `HdfsWriter.channelRead0`.
- Ops `webhdfs_read`, `webhdfs_create`, `webhdfs_append`, role `client`.
  Event emitted when the stream / writer completes or the channel closes.

Internal leg:

- While inside `onOpen` / `onCreate` / `onAppend`, an advice on the
  `DFSClient` constructor prefixes the client name with `DFSClient_WEBHDFS_`
  (via a copy of the passed `Configuration`, never mutating shared config).
- `DataXceiver` on any DN running the agent maps such ops to role
  `webhdfs_proxy`.
- If tagging is impossible on a given version, a WARN is logged and the
  internal leg stays role `client`.

Client-facing traffic = all `role=client` records; `webhdfs_proxy` is internal
forwarding and can be excluded to avoid double counting.

## 5. Fault tolerance

- All advice uses `suppress = Throwable.class`; the agent never breaks a
  DataNode operation.
- If an expected class or method is not found, a WARN is written to the DN log
  and only that interception is disabled.
- Overhead on the hot path: a ThreadLocal lookup and a `LongAdder.add` per
  packet.

## 6. Testing

- Unit: aggregator, role/locality derivation, `maxUsers` cap.
- Build and tests run on JDK 8 (Hadoop 3.1/3.3 do not run on JDK 24+).
- Integration: 3-DN `MiniDFSCluster` with block tokens under simple auth (same
  user-extraction path as Kerberos), agent installed via
  `ByteBuddyAgent.install()`; plus one Kerberos smoke test on Hadoop's
  `SaslDataTransferTestCase` (MiniKdc). Distinct users write and read files:
  - `role=client` bytes equal file size;
  - `role=pipeline` bytes equal `(replication - 1) × file size`;
  - WebHDFS read/write via `WebHdfsFileSystem`: `webhdfs_*` bytes equal file
    size, internal leg tagged `webhdfs_proxy`.
- Run in both Maven profiles (`hadoop-3.1.1`, `hadoop-3.3.6`).

## Out of scope

- Block-to-file mapping (step 2).
- Sinks other than metrics2 / JMX.
- Exact short-circuit read bytes (not observable on the DataNode).
