# hdfs-traffic-agent

[Русская версия](README.md)

This repository accounts HDFS DataNode traffic per user and per Hive table. This document
describes what is counted and how: dimensions, JMX metrics, event fields, report tables and
known limitations, plus the build and the Docker stand. Installation on a cluster,
configuration and operation are described in the [admin guide](docs/admin-guide.en.md).

It has three parts:

- `agent/`: a Java agent loaded into every DataNode. It writes one JSONL event per operation
  and can publish per-user counters in JMX.
- `block-mapper/`: a tool that turns an fsimage into tables (block → file, file → directory).
- `spark-jobs/`: two Spark jobs that join the events with those tables and write per-minute
  traffic by user and table into Hive.

Supported: Apache Hadoop 3.3.x and HDP 3.1 (Hadoop 3.1.1), Java 8. The reports use Spark 3.5.1
(Scala 2.12); Spark 2 is not supported.

Placeholders in this document:

| Placeholder | Meaning |
|---|---|
| `<version>` | project version from `pom.xml`, for example `0.1.0-SNAPSHOT` |
| `<dn_host>`, `<dn_http_port>` | DataNode host and its HTTP port (`dfs.datanode.http.address`) |
| `<root>` | HDFS directory of the reports, default `/data/hdfs-traffic` |
| `<dt>` | a UTC day, `YYYY-MM-DD` |

## Counted dimensions

| Dimension | Values |
|-----------|--------|
| `user`    | user from the block access token; `__unknown__` without a token; `__other__` above `maxUsers` |
| `op`      | `read`, `write`, `transfer`, `copy`, `replace`, `scr_fds`, `webhdfs_read`, `webhdfs_create`, `webhdfs_append` |
| `role`    | `client`, `pipeline` (replica forwarding), `datanode` (re-replication, balancer, recovery), `webhdfs_proxy` (internal leg of WebHDFS) |
| `locality`| `local`, `remote` |

`__unknown__` means no block token, or a token without a user. Balancer and Mover generate
tokens without a user, so their `replace` and `copy` traffic is counted as `__unknown__`.

Counters per series:

| Counter | What it counts |
|---|---|
| `Bytes` | payload bytes, without checksums and headers |
| `EstimatedBytes` | short-circuit reads: the block length per descriptor request (the DataNode does not see the bytes the client reads) |
| `Ops` | completed operations |

Counters are cumulative since the DataNode start.

Client traffic is the sum of the `role=client` series. `webhdfs_proxy` repeats the
`webhdfs_*` HTTP traffic on the internal DataTransferProtocol leg: exclude it, or WebHDFS
bytes are counted twice.

## Build

Requirements: a full JDK 8 (with `lib/tools.jar`) and Maven.

```bash
export JAVA_HOME=/path/to/jdk8
mvn clean verify -P hadoop-3.1.1   # HDP 3.1
mvn clean verify -P hadoop-3.3.6   # Apache Hadoop 3.3.x (the default profile)
```

Run one build, with the profile of the cluster's Hadoop line. The profile sets the Hadoop
version the tests run against (MiniDFSCluster, MiniKDC). Hadoop is a `provided` dependency
and is not bundled. The jars of the agent and `spark-jobs` are the same for both lines;
`block-mapper` has to be built for the cluster's line (see
[Block-to-file mapping](docs/admin-guide.en.md#block-to-file-mapping-block-mapper)).

Build distributable jars with `clean`: without it the shade plugin re-shades the already
shaded jar.

Output:

| Module | Jar |
|---|---|
| agent | `agent/target/hdfs-traffic-agent-<version>.jar` |
| block-mapper | `block-mapper/target/block-mapper-<version>-hadoop-<hadoop version>.jar` |
| spark-jobs | `spark-jobs/target/spark-jobs-<version>.jar` |

## Metrics

JMX bean `Hadoop:service=DataNode,name=UserTraffic`. Its content depends on the options:

| Options | Bean content |
|---|---|
| `userMetrics=true` | per-user counters (below) and, when `events` is set, the writer thread counters (see [Event files](docs/admin-guide.en.md#event-files)) |
| `userMetrics=false` (default), `events` set | the writer thread counters only: `EventsWritten`, `EventsDropped`, `EventFilesDropped`, `EventsQueueSize` |
| `userMetrics=false`, no `events` | the bean is not registered; the agent logs a WARN that traffic is recorded nowhere |

With `userMetrics=false` the agent keeps no per-user counters in memory; per-user traffic is
in the events only.

Per-user counters: attribute names have the form
`<Counter>.user.<u>.op.<op>.role.<role>.locality.<loc>` (as in nntop), for example
`Bytes.user.alice.op.read.role.client.locality.remote`. Over HTTP:

```bash
curl -s 'http://<dn_host>:<dn_http_port>/jmx?qry=Hadoop:service=DataNode,name=UserTraffic'
```

Hadoop's `/jmx` servlet drops attributes whose name contains `=`, `:` or a space without an
error, so the agent replaces these characters in user names with `_`. A user name may
contain dots: parse the name from the right, as the jmx_exporter rule in the admin guide does.

Cardinality of the per-user counters and the Ambari Metrics and Prometheus setup are
described in the [admin guide](docs/admin-guide.en.md#metrics-in-a-monitoring-system).

## Events (JSONL)

With the `events` option the agent also writes an event log: one JSON line per emission,
fields in this order:

| Field | Meaning |
|---|---|
| `ts` | emission time, ms since the epoch |
| `start` | operation start time, ms |
| `dn` | DataNode host name, resolved once on the writer thread; `unknown` if the lookup fails |
| `op_id` | `<6 hex>-<n>`, shared by all events of one operation; unique per DataNode only, so across the cluster an operation is the pair (`dn`, `op_id`) |
| `user` | user as in the metrics, not capped by `maxUsers` |
| `op`, `role`, `locality` | same values as in the metrics |
| `bpid` | block pool id |
| `block_id` | block id; `-1` when there is no block |
| `file_block_id` | block group id for erasure-coded (striped) blocks, otherwise the block id |
| `bytes`, `est_bytes` | delta since the previous event of the same operation; the total is the sum over `op_id` |
| `dur_ms` | time from the operation start to this event, ms |
| `last` | `true` on the event that closes an operation; counts as one op |
| `peer` | remote address |
| `path` | WebHDFS HTTP-leg events only: the path the DataNode was asked for, for example a `._COPYING_` temporary name during `hdfs dfs -put` |

A long operation emits an event every `flushIntervalSec`, so a block that stays open for an
hour shows up as a series of events, not as one spike at close.

Where the event files live and how they are rotated and shipped into HDFS is described in
the [admin guide](docs/admin-guide.en.md#events-local-files-and-shipping-into-hdfs).

## Reports (spark-jobs)

`spark-jobs` has two Spark jobs. They turn the shipped events and the block-mapper output
into per-minute traffic by user and table.

What the jobs do:

- `CompactEvents` reads `<root>/raw/dt=<dt>/dn=*/*.jsonl` and writes `<root>/events/dt=<dt>`.
  Malformed lines are counted and skipped; if every line is malformed, the job fails and does
  not overwrite the partition. `*._COPYING_` files of an unfinished `put` are not read. A
  missing raw partition fails the job.
- `ResolvePaths` joins the events of `<dt>` with the fsimage model and writes
  `<root>/event_path/dt=<dt>` (events plus `inode_id` and `table_dir`, with `path` filled in)
  and `<root>/traffic_minute/dt=<dt>`.
  - The image is `--image-dt`; by default the latest `fsimage/dt=<dt>` whose `meta/` holds an
    `.orc` file (hidden temporary directories are skipped).
  - A file created on `<dt>` appears only in a checkpoint taken after `<dt>` ends. So the
    daily script passes `--image-dt <today>`, the image fetched after a post-midnight
    checkpoint. When the image date is not after `<dt>`, the job logs a WARN: files created
    on `<dt>` may resolve to `?`.
  - Paths are the current paths as of that checkpoint, with renames applied.
  - WebHDFS events keep the `path` from the event. Blocks that are not in the image get `?`.

Building and running the jobs, the daily report and the Spark setup on HDP are described in
the [admin guide](docs/admin-guide.en.md#reports-running-the-jobs-spark-jobs).

Tables:

| Table | Columns (all partitioned by `dt`) |
|---|---|
| `events` | the event fields of the JSONL (`ts`, `start`, `dn`, `op_id`, `user`, `op`, `role`, `locality`, `bpid`, `block_id`, `file_block_id`, `bytes`, `est_bytes`, `dur_ms`, `last`, `peer`, `path`) |
| `fs_inode`, `fs_edge`, `fs_block_inode`, `fs_meta` | the block-mapper tables; `dt` is the image date |
| `event_path` | the `events` columns plus `inode_id`, `table_dir` |
| `traffic_minute` | `minute` (epoch ms of the UTC minute start), `user`, `op`, `role`, `locality`, `table_dir`, `bytes`, `est_bytes`, `ops` (number of `last` events) |

All tables are EXTERNAL ORC with ZLIB. The DDL is in `hive/schema.sql`, generated from
`org.hadooptraffic.spark.Ddl`; the jobs create the tables with `CREATE ... IF NOT EXISTS`.
The jobs add partitions with `ALTER TABLE ... ADD IF NOT EXISTS PARTITION`.

The jobs read the JSONL and ORC files directly; there is no Hive table over the raw files.

### Table attribution (`table_dir`)

`table_dir` is computed from the path, without the Hive metastore:

1. Drop the file name.
2. Cut the directory at the first component of one of these kinds:
   - partition: `key=value`;
   - service: starts with `.` or `_` (`.hive-staging_*`, `_temporary`);
   - Hive ACID: `base_N`, `delta_N_N`, `delete_delta_N_N`, each with an optional `_v<n>`
     suffix.

Effects:

- Files written into `.hive-staging_*` and then moved into the table are counted for the
  table: the image has their final path.
- Files deleted before the next NameNode checkpoint (Hive scratch directories, job staging
  jars, temporary outputs) are not in the image and get `?`.
- An unpartitioned table with subdirectories gets the subdirectory as `table_dir`, one level
  below the table.

### Query example

Top 20 (minute, table) pairs by client write rate for one hour, MB/s:

```sql
SELECT table_dir, SUM(bytes) / 1048576 / 60 AS mb_per_s, minute
FROM hdfs_traffic.traffic_minute
WHERE dt = '2026-11-06'
  AND op IN ('write', 'webhdfs_create', 'webhdfs_append') AND role = 'client'
  AND minute >= 1793923200000 AND minute < 1793926800000   -- 2026-11-06 00:00-01:00 UTC
GROUP BY minute, table_dir
ORDER BY mb_per_s DESC
LIMIT 20;
```

`user` and `start` are reserved words in Hive 3: write them in backquotes in beeline.

## Docker stand

`stand/` runs the agent on a secure cluster in Docker: an MIT KDC, a NameNode, three
DataNodes started with `-javaagent`, and a client container. Kerberos, block access tokens
and SASL data transfer protection are on. Two distributions:

- HDP 3.1.0.0-78, from a local copy (its jars may not be redistributed);
- Apache Hadoop 3.3.6, copied out of the `apache/hadoop:3.3.6` image.

Requirements: Docker with Compose, JDK 8, for HDP a local copy of the distribution in
`HDP_DIST_DIR` (default `~/hdp/3.1.0.0-78`).

```bash
mvn clean package -P hadoop-3.1.1      # the stand mounts agent/target/*.jar
cd stand
./prepare.sh hdp
docker compose --env-file .env.hdp up -d --build
./run-scenario.sh hdp
docker compose --env-file .env.hdp down -v
```

For Apache Hadoop use `apache` and `.env.apache`.

The stand runs the agent with `userMetrics=true` by default because it checks the `/jmx`
counters; keep this option in your own `AGENT_ARGS`.

`run-scenario.sh` drives known traffic, one user per step: a client write with replication
3, re-replication after `setrep`, remote and local reads, a WebHDFS write and read. Then it
checks:

- all 8 hooked classes are instrumented;
- the `UserTraffic` delta, summed over the DataNodes' `/jmx`, equals the expected byte
  counts;
- the events shipped with `upload-events.sh` give the same byte counts and the expected
  WebHDFS paths.

Ports on the host: DataNode `/jmx` on 29864–29866, NameNode UI (SPNEGO) on 29870.

## Known issues of the agent

- Write bytes are reported every `flushIntervalSec` while a block is open and the rest at
  close. Bytes not yet reported when the DataNode restarts are lost.
- Exact bytes of short-circuit reads are not visible. `EstimatedBytes` counts the block
  length once per descriptor request; later reads from the client's descriptor cache are not
  counted. WebHDFS reads of local replicas may use short-circuit inside the DataNode and then
  appear as `op=scr_fds, role=client` instead of `webhdfs_proxy`.
- Positional reads not aligned to checksum chunks can count up to one chunk (512 B by
  default) more than requested.
- Bytes of a write packet that fails mid-receive are not counted.
- A WebHDFS stream that is never closed loses its event. WebHDFS reads report their bytes at
  stream close, without intermediate deltas.
- A late `addBytes` racing with `finish()` on another thread can lose those bytes, in JMX
  and in the events.
- DataTransferProtocol locality compares the socket's own addresses (the client connected to
  the same IP it came from). A local client on another IP of a multi-homed host is counted
  as `remote`.
- Events carry block ids only; file paths come from the fsimage mapping (block-mapper, see
  `docs/superpowers/specs/2026-11-06-block-to-file-mapping-design.md`).
