# HDFS DataNode per-user traffic accounting — design

Date: 2026-11-06
Status: draft, awaiting review

This document describes the design of `hdfs-traffic-agent`, a Java agent that
counts DataNode traffic per user and publishes the counters through Hadoop
metrics2 and JMX. It is written for the developers of the agent and for the
reviewers of this design.

## Scope and metrics

The agent counts bytes and operations per combination of four dimensions:

| Dimension | Values |
|---|---|
| user | user name from the block token |
| operation | see [Operation types](#operation-types) |
| role | `client`, `pipeline`, `datanode`, `webhdfs_proxy` |
| locality | `local`, `remote` |

Step 2 (not covered here) maps blocks to file names and may add new sinks. The
event model below keeps `blockPoolId` and `blockId` for that step.

## Supported versions and prerequisites

- Apache Hadoop 3.3.x and HDP 3.1 (Hadoop 3.1.1), Java 8.
- Kerberos enabled and `dfs.block.access.token.enable=true`: the user is
  taken from `BlockTokenIdentifier.getUserId()`.

## 1. Build and deployment

- Standalone Maven project `hdfs-traffic-agent`. Output: one shaded jar with
  ByteBuddy relocated to avoid classpath conflicts. Bytecode target: Java 8.
- Hadoop dependencies are `provided`. The Maven profiles `hadoop-3.1.1` and
  `hadoop-3.3.6` compile and test against both Hadoop lines.
- Deployment: add `-javaagent:<agent_jar>=maxUsers=2000` to
  `HADOOP_DATANODE_OPTS`, where `<agent_jar>` is the full path to the agent
  jar. On HDP set it in Ambari, in `hadoop-env`. No Hadoop jar is modified.

Agent option:

| Parameter | Default | What it does | When to change |
|---|---|---|---|
| `maxUsers` | 2000 | number of distinct users tracked in JMX; further users are aggregated into `__other__` | when users you need to see separately land in `__other__` |

## 2. Interception of DataTransferProtocol

The agent installs advice on these methods of
`org.apache.hadoop.hdfs.server.datanode.DataXceiver`: `readBlock`,
`writeBlock`, `transferBlock`, `copyBlock`, `replaceBlock`,
`requestShortCircuitFds`. Arguments are captured with `@AllArguments` and
located by type, not by index, because the signatures differ between Hadoop
3.1 and 3.3.

### Operation context

On method entry the advice puts an `OpContext` into a ThreadLocal:

| Field | Value |
|---|---|
| `user` | `BlockTokenIdentifier.getUserId()`; `__unknown__` if no token can be decoded |
| `op` | operation type, see below |
| `role` | see [Role rules](#role-rules) |
| `locality` | `local` if the peer is a domain socket or its address belongs to a local interface, otherwise `remote` |
| `blockPoolId`, `blockId` | block of the operation |
| peer address, start time | for the event |

### Operation types

| `DataXceiver` method | `op` |
|---|---|
| `readBlock` | `read` |
| `writeBlock` | `write` |
| `transferBlock` | `transfer` |
| `copyBlock` | `copy` |
| `replaceBlock` | `replace` |
| `requestShortCircuitFds` | `scr_fds` |

WebHDFS adds `webhdfs_read`, `webhdfs_create` and `webhdfs_append` (section 4).

### Role rules

The first matching rule wins:

1. `copyBlock`, `replaceBlock`, `transferBlock` → `datanode`.
2. Empty `clientName`, any operation → `datanode` (transfer started by a
   DataNode, EC reconstruction).
3. `writeBlock` with stage `TRANSFER_RBW` or `TRANSFER_FINALIZED` → `datanode`.
4. `writeBlock` with pipeline position `pipelineSize - 1 - targets.length`
   greater than 0 → `pipeline`.
5. `clientName` starts with `DFSClient_WEBHDFS_` → `webhdfs_proxy`.
6. Otherwise → `client`.

### Byte counting

Bytes are added to the current `OpContext` per packet, from the return values
of `BlockSender.sendPacket` and `BlockReceiver.receivePacket`. Bytes of an
aborted transfer are counted up to the abort. Only payload bytes are counted,
without checksums and headers. The return-value semantics of these private
methods are checked per Hadoop version in the compatibility tests.

Replication ordered by the NameNode runs in `DataNode$DataTransfer.run`,
outside `DataXceiver`. The agent intercepts it as operation `transfer`, role
`datanode`, user from its block token (usually the service user `hdfs`).

Short-circuit reads bypass the DataNode data path. For them only operation
`scr_fds` is counted, with `estimatedBytes` set to the block length. This value
is an estimate.

## 3. Event model and JMX output

On method exit, normal or by exception, the agent emits
`TrafficEvent{user, op, role, locality, bpid, blockId, bytes, estimatedBytes,
durationMs, peer}` to the `TrafficSink` interface. Step 1 has one
implementation, the aggregator. Block-to-file mapping in step 2 is another
sink; interception does not change for it.

### Aggregator

`ConcurrentHashMap<TrafficKey(user, op, role, locality), TrafficCounters>`
with `LongAdder` counters for bytes, estimated bytes and operations. Counters
are cumulative since DataNode start, so Prometheus `rate()` can be applied to
them.

When `maxUsers` distinct users are tracked, events of new users are aggregated
into `__other__`.

### metrics2 source and JMX names

The metrics2 source `UserTraffic` is registered on exit of
`DataNode.startDataNode`, when the metrics system is already initialised.

- JMX bean: `Hadoop:service=DataNode,name=UserTraffic`, one record.
- Attribute name: `<counter>.user.<user>.op.<op>.role.<role>.locality.<locality>`,
  where `<counter>` is `Bytes`, `EstimatedBytes` or `Ops` and the other
  placeholders are the dimension values. nntop uses the same scheme.
- Names contain no `=`, `:` or spaces: Hadoop's `/jmx` servlet drops such
  attributes without an error (found on the Docker stand).
- Tags are not used: metrics2 flattens a source with several records into
  suffixed JMX attributes (`Bytes.1`, …), and Ambari Metrics ignores tags.

## 4. WebHDFS

The DataNode serves WebHDFS through the Netty `WebHdfsHandler`, which uses an
internal `DFSClient` running as the end user. WebHDFS traffic has two legs:

1. HTTP leg: external client ↔ DataNode.
2. Internal leg: internal `DFSClient` ↔ `DataXceiver` on the same or another
   DataNode.

### HTTP leg

- Advice on `WebHdfsHandler.onOpen`, `onCreate`, `onAppend` takes the user from
  `UserGroupInformation.getCurrentUser()` (the handler already runs in
  `doAs`) and the locality from `ctx.channel().remoteAddress()`. The context
  stays in a ThreadLocal for the duration of the call.
- Read: the handler's anonymous `ChunkedStream` subclass (`WebHdfsHandler$N`)
  is bound to the context through a weak map in its constructor. On its
  `close()` the byte count is taken from `ChunkedStream.transferredBytes()`.
  Netty itself is not instrumented.
- Write: the `HdfsWriter` constructor is bound the same way; bytes are summed
  from `HttpContent.content().readableBytes()` in `HdfsWriter.channelRead0`.
- Operations `webhdfs_read`, `webhdfs_create`, `webhdfs_append`, role
  `client`. The event is emitted when the stream or writer completes or the
  channel closes.

### Internal leg

- While a thread is inside `onOpen`, `onCreate` or `onAppend`, advice on the
  `DFSClient` constructor prefixes the client name with `DFSClient_WEBHDFS_`.
  It changes a copy of the passed `Configuration`, not the shared one.
- `DataXceiver` on every DataNode running the agent maps such operations to
  role `webhdfs_proxy`.
- If tagging fails on a Hadoop version, the agent logs a WARN and the internal
  leg stays role `client`.

Client-facing traffic is the sum of all `role=client` records.
`webhdfs_proxy` is internal forwarding; exclude it to avoid counting WebHDFS
bytes twice.

## 5. Failure handling

- All advice uses `suppress = Throwable.class`: an exception in the agent does
  not reach the DataNode operation.
- If an expected class or method is not found, the agent writes a WARN to the
  DataNode log and disables only that interception.
- Cost on the data path: one ThreadLocal lookup and one `LongAdder.add` per
  packet.

## 6. Tests

- Unit: aggregator, role and locality derivation, `maxUsers` cap.
- Build and tests run on JDK 8; Hadoop 3.1 and 3.3 do not run on JDK 24+.
- Integration: 3-DataNode `MiniDFSCluster` with block tokens under simple auth
  (same user-extraction path as Kerberos), agent installed with
  `ByteBuddyAgent.install()`. One Kerberos smoke test runs on Hadoop's
  `SaslDataTransferTestCase` (MiniKdc). Different users write and read files,
  and the tests check:
  - `role=client` bytes equal the file size;
  - `role=pipeline` bytes equal `(replication - 1) × file size`;
  - WebHDFS read and write through `WebHdfsFileSystem`: `webhdfs_*` bytes
    equal the file size, the internal leg is tagged `webhdfs_proxy`.
- Tests run in both Maven profiles, `hadoop-3.1.1` and `hadoop-3.3.6`.

## Out of scope

- Block-to-file mapping (step 2).
- Sinks other than metrics2 and JMX.
- Exact short-circuit read bytes: the DataNode does not see them.
