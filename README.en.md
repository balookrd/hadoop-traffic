# hdfs-traffic-agent

[Русская версия](README.md)

This repository accounts HDFS DataNode traffic per user and per Hive table. It is for HDFS
administrators who install it on a cluster and analysts who query the reports. It has three
parts:

- `agent/`: a Java agent loaded into every DataNode. It counts bytes per user in JMX and can
  write one JSONL event per operation.
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
| `<nn_host>` | host of the active NameNode |
| `<realm>` | Kerberos realm |
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
[Block-to-file mapping](#block-to-file-mapping-block-mapper)).

Build distributable jars with `clean`: without it the shade plugin re-shades the already
shaded jar.

Output:

| Module | Jar |
|---|---|
| agent | `agent/target/hdfs-traffic-agent-<version>.jar` |
| block-mapper | `block-mapper/target/block-mapper-<version>-hadoop-<hadoop version>.jar` |
| spark-jobs | `spark-jobs/target/spark-jobs-<version>.jar` |

## Agent installation

Requirement: `dfs.block.access.token.enable=true`. It is always on with Kerberos; without it
every user is counted as `__unknown__`.

On every DataNode:

1. Copy the jar to `/opt/hdfs-traffic-agent/hdfs-traffic-agent.jar`.
2. Add the agent to the DataNode JVM options in `hadoop-env.sh`. On HDP edit it in Ambari:
   **HDFS → Configs → Advanced hadoop-env → hadoop-env template**.

   ```bash
   export HDFS_DATANODE_OPTS="$HDFS_DATANODE_OPTS -javaagent:/opt/hdfs-traffic-agent/hdfs-traffic-agent.jar=events=/var/log/hadoop/hdfs-traffic/events.jsonl"
   ```

   Without options the agent records nothing: `events` turns on the events, and
   `userMetrics=true` the per-user JMX counters (see [Agent options](#agent-options)).

   Hadoop 3 also reads the deprecated name `HADOOP_DATANODE_OPTS`.
3. Restart the DataNode.

   Check the DataNode log: it has the line `hdfs-traffic-agent installed` and one
   `hdfs-traffic-agent: instrumented ...` line per hooked class. A `WARN ... hook is disabled`
   line means a method was not found in this Hadoop build; that hook does not count, the
   others do.

### Agent options

Options go after `=` in `-javaagent`, as comma-separated `key=value`. An unknown or invalid
option disables the agent with a WARN in the DataNode log; the DataNode starts anyway.

| Option | Default | What it does | When to change |
|---|---|---|---|
| `userMetrics` | `false` | `true` publishes per-user counters in JMX (see [Metrics](#metrics)) | turn it on when the per-user series are needed in the metrics system and it can take up to ~60k attributes per DataNode |
| `maxUsers` | 2000 | only with `userMetrics=true`: users that get their own JMX series, first come first served; the rest go to `__other__` | lower it when the metrics sink cannot take ~60k attributes per DataNode |
| `events` | off | path of the active JSONL events file, for example `/var/log/hadoop/hdfs-traffic/events.jsonl`; no commas | set it to enable the events |
| `eventsQueue` | 65536 | events buffered for the writer thread; overflow is dropped and counted in `EventsDropped` | raise it when `EventsDropped` grows |
| `eventsMaxFileMb` | 256 | size at which the active file is rotated, MB | |
| `eventsRotateSec` | 300 | age at which the active file is rotated, seconds | |
| `eventsMaxFiles` | 100 | closed files kept while not shipped; beyond it the oldest are deleted and counted in `EventFilesDropped` | raise it if shipping can stop for longer than the buffer lasts (see [Local buffer on a DataNode](#local-buffer-on-a-datanode)) |
| `flushIntervalSec` | 10 | long operations report their bytes every N seconds (JMX and events); 0 reports them at the end only | |

## Metrics

JMX bean `Hadoop:service=DataNode,name=UserTraffic`. Its content depends on the options:

| Options | Bean content |
|---|---|
| `userMetrics=true` | per-user counters (below) and, when `events` is set, the writer thread counters (see [Event files](#event-files)) |
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
contain dots: parse the name from the right, as the jmx_exporter rule below does.

Cardinality: a user has one series per (op, role, locality) combination seen, usually 8–10,
times 3 counters. With `maxUsers=2000` that is up to ~50–60k attributes per DataNode. The
source goes to every metrics2 sink, not only JMX. On HDP the Ambari Metrics sink
(`datanode.sink.timeline`) is on by default; to keep `UserTraffic` out of it, add to
`hadoop-metrics2.properties`:

```properties
datanode.sink.timeline.source.filter.class=org.apache.hadoop.metrics2.filter.GlobFilter
datanode.sink.timeline.source.filter.exclude=UserTraffic
```

Prometheus jmx_exporter rule. With `lowercaseOutputName` the names become
`hdfs_datanode_user_traffic_bytes_total` and so on:

```yaml
lowercaseOutputName: true
rules:
  - pattern: 'Hadoop<service=DataNode, name=UserTraffic><>(Bytes|EstimatedBytes|Ops)\.user\.(.+)\.op\.(\w+)\.role\.(\w+)\.locality\.(\w+)'
    name: hdfs_datanode_user_traffic_$1_total
    type: COUNTER
    labels:
      user: "$2"
      op: "$3"
      role: "$4"
      locality: "$5"
```

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

### Event files

The active file has the name from the `events` option, for example `events.jsonl`. It is
rotated by size or age into `events-<yyyyMMdd-HHmmss>-<seq>.jsonl`, named by the UTC time of
its first event. A file never spans a UTC day of `ts`: the date in the name is the date of
all its events (events reordered by a few milliseconds across midnight stay in the earlier
file). A non-empty active file left over from a crash is rotated at agent start and dated by
its first event. `eventsMaxFiles` counts and deletes only names of that exact pattern.

The DataNode user must be able to create the events directory. The agent creates it on the
first write and retries on failure.

The DataNode never waits for the writer: when the queue is full or a write fails, events are
dropped and counted, with a WARN at most once a minute. Attributes of the `UserTraffic` bean
for monitoring:

| Attribute | Type | Meaning |
|---|---|---|
| `EventsWritten` | counter | events written to files |
| `EventsDropped` | counter | events dropped (queue full, I/O error) |
| `EventFilesDropped` | counter | closed files deleted by `eventsMaxFiles` before shipping |
| `EventsQueueSize` | gauge | events waiting for the writer thread |

### Shipping events into HDFS

`deploy/upload-events.sh` puts closed files into HDFS as
`<raw_root>/dt=<dt>/dn=<host>/<file>` and deletes a local file only after its put succeeded.

```
upload-events.sh [-d events_dir] [-r hdfs_root] [-n file_prefix] [-k keytab -p principal]
                 [-H host] [-b batch_files] [-j max_jitter_sec]
```

| Option | Default | What it does |
|---|---|---|
| `-d` | `/var/log/hadoop/hdfs-traffic` | events directory (the directory of the agent's `events` file) |
| `-r` | `/data/hdfs-traffic/raw` | HDFS root of the raw files (`<raw_root>`) |
| `-n` | `events` | file name prefix (the name of the `events` file without `.jsonl`) |
| `-k`, `-p` | none | keytab and principal for `kinit`; omit both without Kerberos |
| `-H` | `hostname -f` | node key in the `dn=` partition; it may differ from the `dn` field inside the events |
| `-b` | 100 | files per `hdfs dfs -put` |
| `-j` | 0 | sleep a random 0..N seconds before starting |

How a run works:

- With no closed files it exits without `kinit` and without starting a JVM.
- It runs one `hdfs dfs -mkdir` for all target directories and one `hdfs dfs -put` per target
  directory and batch of `-b` files, so a run starts two or three JVMs, not one per file.
- It takes `flock` on `<events_dir>/.upload.lock`, so runs never overlap, and keeps its
  Kerberos ticket in `<events_dir>/.upload.krb5cc`.
- A failed batch does not stop the run: the other batches are shipped, the failed files stay
  for the next run and the exit code is 1. The next run puts them again with `-f`; this is
  safe because closed files never change.

The writes of the upload into HDFS appear as traffic of the user it runs as.

### Running the upload from a systemd timer

`deploy/systemd/` has a oneshot service and a timer:

- The timer starts the service every 5 minutes with a random delay of up to 4 minutes
  (`RandomizedDelaySec=240`), so the DataNodes of a cluster do not hit the NameNode and the
  KDC in the same second. It does not start a run while the previous one is still going.
- The service runs as `hdfs` with `Nice=10`, I/O class best-effort priority 7,
  `HADOOP_CLIENT_OPTS=-Xmx256m` and `TimeoutStartSec=30min`.

Install on every DataNode, as root, from a checkout of this repository:

1. Install the script and the units:

   ```bash
   install -d /opt/hdfs-traffic-agent
   install -m 0755 deploy/upload-events.sh /opt/hdfs-traffic-agent/
   install -m 0644 deploy/systemd/hdfs-traffic-upload.service \
       deploy/systemd/hdfs-traffic-upload.timer /etc/systemd/system/
   install -m 0644 deploy/systemd/hdfs-traffic-upload.env /etc/default/hdfs-traffic-upload
   ```

2. In `/etc/default/hdfs-traffic-upload` set the script options in `UPLOAD_ARGS`: events
   directory, HDFS root, keytab and principal. `JAVA_HOME` and `HADOOP_CLIENT_OPTS` can be set
   there too.

   The unit looks for `hdfs` in `/usr/hdp/current/hadoop-client/bin`, `/opt/hadoop/bin` and
   `/usr/local/bin:/usr/bin:/bin`. For another location override `PATH` with
   `systemctl edit hdfs-traffic-upload.service`.

   `RandomizedDelaySec` needs systemd 229 or newer. Older systemd (RHEL/CentOS 7 has 219)
   ignores it with a warning in the journal, and all nodes start at the same second: add
   `-j 240` to `UPLOAD_ARGS`.
3. Enable the timer:

   ```bash
   systemctl daemon-reload
   systemctl enable --now hdfs-traffic-upload.timer
   ```

4. Check it:

   ```bash
   systemctl list-timers hdfs-traffic-upload.timer   # time of the next run
   systemctl start hdfs-traffic-upload.service       # run now
   journalctl -u hdfs-traffic-upload.service         # shipped files and errors
   ```

A failed run leaves the service in the `failed` state until the next successful run; monitor
it as any other unit.

Without systemd, use cron with `-j`, so that the nodes do not start together. One entry per
line:

```
*/5 * * * * hdfs /opt/hdfs-traffic-agent/upload-events.sh -j 240 -d /var/log/hadoop/hdfs-traffic -r /data/hdfs-traffic/raw -k /etc/security/keytabs/hdfs.headless.keytab -p hdfs@<realm>
```

## Running on a large cluster

This section is for enabling the agent on hundreds of DataNodes. On each node the agent adds
a fraction of a microsecond per operation. The limits on such a cluster are elsewhere: the
total event volume, the number of metric series, and raw files in HDFS that nothing deletes.
Estimate them before the rollout and check them on a pilot.

The figures below come from a synthetic run of the agent's data path without a DataNode:
JDK 8, 11 cores, a local SSD, read events of users `user0`…`user199`. On that machine the
writer thread wrote ~735k events/s; what it could not keep up with went to `EventsDropped`,
and the operation threads never waited.

### Estimating the event volume

The agent writes one event per operation: per DataXceiver operation (`readBlock`,
`writeBlock`, `transferBlock`, `copyBlock`, `replaceBlock`, `requestShortCircuitFds`), per
`DataNode$DataTransfer` and per WebHDFS request. A long operation also writes an event every
`flushIntervalSec`. The average line in the run is 343 bytes; in real events the length
depends on user names, addresses and `path`.

Estimate the operation rate from DataNode counters that exist without the agent. Read them
twice, 60 seconds apart; the difference divided by 60 is operations per second:

```bash
curl -s 'http://<dn_host>:<dn_http_port>/jmx?qry=Hadoop:service=DataNode,name=DataNodeActivity-*' \
  | grep -E '"(ReadBlockOp|WriteBlockOp|CopyBlockOp|ReplaceBlockOp)NumOps"'
```

`<dn_http_port>` is the DataNode HTTP port (`dfs.datanode.http.address`). If the DataNode
HTTP endpoint requires SPNEGO, add `--negotiate -u :` after `kinit`. Short-circuit descriptor
requests and transfers started by the DataNode itself are not in these four counters, so the
estimate is a lower bound. Take it on several DataNodes with different load: nodes serving
HBase or small positional reads run orders of magnitude more operations than nodes with
large sequential reads.

Volume per day is `operations/s × 343 B × 86,400`:

| Operations/s per DataNode | Per DataNode per day | 500 DataNodes per day | 500 DataNodes in HDFS with replication 3 |
|---|---|---|---|
| 500 | 14.8 GB | 7.4 TB | 22 TB |
| 1,000 | 29.6 GB | 14.8 TB | 44 TB |
| 5,000 | 148 GB | 74 TB | 222 TB |

The pilot gives the exact volume (see [Pilot](#pilot)).

### Local buffer on a DataNode

While shipping does not work (NameNode unavailable, no Kerberos ticket), closed files pile
up in the events directory. The active file is rotated by whichever comes first:
`eventsRotateSec` or `eventsMaxFileMb`. Hence:

- the buffer lasts `eventsMaxFiles × min(eventsRotateSec, eventsMaxFileMb / rate)`;
- it takes up to `eventsMaxFiles × eventsMaxFileMb` of disk, 25.6 GB with the defaults.

| Events/s | Closed file size | Default buffer | Disk |
|---|---|---|---|
| 1,000 | ~103 MB (rotated every 300 s) | ~8.3 h | ~10 GB |
| 5,000 | 256 MB (rotated every ~150 s) | ~4.2 h | 25.6 GB |

Beyond `eventsMaxFiles` the oldest files are deleted and `EventFilesDropped` grows: the
events of that period are lost.

**Warning:** the default directory `/var/log/hadoop/hdfs-traffic` sits next to the DataNode
logs. If that partition is smaller than the disk figure above, it fills up while shipping is
down, and the DataNode can no longer write its log. Move the events directory to a
partition with enough space or lower `eventsMaxFiles`.

### Metrics on a cluster

Per-user counters are off by default (`userMetrics=false`); JMX keeps the four writer thread
counters. They are enough to monitor the agent on any cluster. The rest of this subsection
applies only if you turn on `userMetrics=true`.

With `maxUsers=2000` one DataNode publishes up to ~50–60k `UserTraffic` attributes, and 500
DataNodes up to 25–30 million series. In a run with 2000 users and 16 (op, role, locality)
combinations each, a snapshot took 27 ms and gave 96k attributes; the `/jmx` response was
~8 MB per DataNode. Counters are not reset until the DataNode restarts: a user that once got
a series keeps it.

Before turning on `userMetrics=true`:

- exclude `UserTraffic` from the Ambari Metrics sink (see [Metrics](#metrics)), otherwise all
  per-user series go there;
- for Prometheus, set `maxUsers` to the real number of cluster users and scrape `/jmx` with
  `qry=Hadoop:service=DataNode,name=UserTraffic`, not the whole servlet.

### Raw file retention

No component deletes `<raw_root>/dt=<dt>`: `CompactEvents` only reads it. With rotation every
300 seconds each DataNode puts at least 288 files a day, 500 DataNodes at least 144k files a
day. These are objects in NameNode memory and the space from the volume table.

Delete a raw partition after a successful `CompactEvents`, keeping a few days for
`daily-report.sh -D` reruns. Before deleting, check that the compacted partition exists:

```bash
hdfs dfs -test -d /data/hdfs-traffic/events/dt=<dt> \
  && hdfs dfs -rm -r -skipTrash /data/hdfs-traffic/raw/dt=<dt>
```

`<dt>` is the day as `YYYY-MM-DD`. After the deletion that day can no longer be rerun with
`-D`: `CompactEvents` fails on the missing raw partition, and `events/dt=<dt>` stays as it
was.

### Pilot

1. Estimate the operation rate on several DataNodes and the volume from the table above.
2. Pick the partition for the events directory and `eventsMaxFiles` from the buffer
   formula.
3. Keep `userMetrics=false` (the default). If the per-user series are needed, turn them on
   on the pilot first and measure the load on the metrics system (see
   [Metrics on a cluster](#metrics-on-a-cluster)).
4. Enable the agent on 5–10 DataNodes (see [Agent installation](#agent-installation)) and the
   upload timer on the same nodes.
5. After a day, check on every pilot DataNode:

   - `EventsDropped` and `EventFilesDropped` are 0, `EventsQueueSize` is close to 0;
   - `journalctl -u hdfs-traffic-upload.service` has no errors, the events directory does
     not grow;
   - `ReadBlockOpAvgTime` and `WriteBlockOpAvgTime` from `DataNodeActivity-*`, DataNode CPU
     and GC pauses are at the level of the day before the agent.

6. Compute the real volume per node and multiply by the number of DataNodes:

   ```bash
   hdfs dfs -du -s -h /data/hdfs-traffic/raw/dt=<dt>/dn=<host>
   ```

   `<host>` is the node key from `upload-events.sh -H`, by default the DataNode's
   `hostname -f`.
7. Set up raw file retention before enabling the agent on the whole cluster.

Rollback: remove `-javaagent:...` from `HDFS_DATANODE_OPTS` and restart the DataNode. Keep the
upload timer until the events directory is empty, then run
`systemctl disable --now hdfs-traffic-upload.timer`.

## Block-to-file mapping (block-mapper)

Events carry block ids only. `block-mapper` reads a downloaded fsimage and writes the inode
model: block → inode, inode → parent and name. The Spark jobs use it to turn an event's
`file_block_id` into the current path of the file (see [Reports](#reports-spark-jobs)).

Build it with the profile of the cluster's Hadoop line: the generated protobuf classes of
the fsimage differ between 3.1 and 3.3, and a jar built for one line does not read the other.

```bash
mvn clean verify -P hadoop-3.1.1      # HDP
mvn clean verify -P hadoop-3.3.6      # Apache Hadoop 3.3.x
```

The jar bundles `orc-core` 1.5.13, `orc-shims`, `hive-storage-api`, `aircompressor`,
`commons-lang` 2.6 and `threeten-extra`, relocated under `org.hadooptraffic.mapper.shaded`,
so Hive jars on `HADOOP_CLASSPATH` do not replace them. `protobuf-java` 2.5.0 comes from
Hadoop. Hadoop 3.4+ (where protobuf 2.5 is optional) is not supported.

Run it on a downloaded image, never on the NameNode's storage directory. Run both commands
as an HDFS cluster administrator (a member of `dfs.cluster.administrators`, for example
`hdfs`) on any host with an HDFS client:

```bash
hdfs dfsadmin -fetchImage /data/fsimg
hadoop jar block-mapper-<version>-hadoop-3.1.1.jar org.hadooptraffic.mapper.FsImageMapper \
  -i /data/fsimg/fsimage_<txid> -o hdfs:///data/hdfs-traffic/fsimage/dt=<dt> [--format tsv]
```

`-i` is a local file, `<txid>` is the transaction id in the downloaded file name. `-o` is
any Hadoop FS URI.

`fetchImage` returns the last NameNode checkpoint, not the live namespace: files created or
renamed after the checkpoint are not in the image. The daily script waits for a fresh
checkpoint (see [Daily run](#daily-run)).

**Warning:** do not call `hdfs dfsadmin -saveNamespace` on a production NameNode to get a
newer image. It needs safe mode, and in safe mode the NameNode rejects all writes.

Output: one file per table, `<out>/<table>/part-00000.orc`:

| Table | Columns |
|---|---|
| `inode` | `inode_id`, `type` (`F` file, `D` directory, `S` symlink), `name` |
| `edge` | `child_id`, `parent_id` |
| `block_inode` | `block_id`, `inode_id` |
| `meta` | `txid`, `image_file`, `parsed_at` (epoch ms) |

ORC (the default): file version 0.12, ZLIB, bloom filters on `inode_id`, `child_id` and
`block_id`; Hive 3.1 and Spark 3.5 read it. `--format tsv` writes `part-00000.tsv` with the
same columns, tab-separated; `\`, tab, newline and carriage return in names are escaped as
`\\`, `\t`, `\n`, `\r`.

The mapper writes into the hidden sibling directory `<parent>/.<name>._tmp`, for example
`fsimage/.dt=2026-11-06._tmp` (Spark and Hive do not take it for a partition), and renames it
to `<out>` on success. An existing `<out>` is never overwritten.

| Exit code | Meaning |
|---|---|
| 0 | success |
| 1 | failure; the message starts with `block-mapper:` |
| 2 | usage error |

Memory: the namespace is read as a stream. Snapshot references and the ORC write buffers
(bounded by ORC's memory manager) are kept in memory.

Known issues:

- Inodes that exist only in snapshots (deleted after a snapshot) have no parent and stay
  unresolved.
- A compressed image saved in parallel (`dfs.image.parallel.save=true` together with
  `dfs.image.compress=true`, Hadoop 3.3) is rejected. Fetch an image saved with one of the
  two settings off.

## Reports (spark-jobs)

`spark-jobs` has two Spark jobs. They turn the shipped events and the block-mapper output
into per-minute traffic by user and table.

```bash
mvn clean verify
```

On HDP 3.1 run the jar with a separate `spark-3.5.1-bin-hadoop3` distribution, not with the
cluster's Spark 2.

Options of both jobs:

| Option | Default | What it does |
|---|---|---|
| `--dt <dt>` | required | the day to process |
| `--root <dir URI>` | `/data/hdfs-traffic` | root of all tables |
| `--db <name>` | `hdfs_traffic` | Hive database |
| `--catalog hive\|none` | `hive` | `none` writes files only and registers nothing in Hive |
| `--image-dt <dt>` | latest image | `ResolvePaths` only: date of the fsimage to use |

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

Each run overwrites only its own `dt` partitions, so a failed run is repeated with the same
`--dt`.

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

**Warning:** do not run `MSCK REPAIR TABLE` on these tables. The four `fs_*` tables have the
same location, `<root>/fsimage`, and the jobs register each partition at
`<root>/fsimage/dt=<dt>/<table>`. For an image date not yet registered, MSCK would add
`dt=<dt>` at `<root>/fsimage/dt=<dt>`, which holds the files of all four tables, and queries
would read files with other schemas. To undo it, run
`ALTER TABLE hdfs_traffic.<table> DROP PARTITION (dt='<dt>')` on the `fs_*` tables (they are
EXTERNAL, the files stay); the next `ResolvePaths` run that uses that image registers the
partition again with the right location.

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

### Spark 3.5.1 on HDP 3.1 (YARN, Kerberos, Hive metastore 3.1)

The stand runs Spark with `--master local` and `--catalog none`, so this setup has not been
tested on a cluster. Do the checks at the end of this section on the first deployment.

1. In `$SPARK_HOME/conf/spark-defaults.conf` (not on a one-off command line, so that cron
   runs get it too):

   ```properties
   spark.sql.hive.metastore.version   3.1.3
   spark.sql.hive.metastore.jars      path
   spark.sql.hive.metastore.jars.path file:///usr/hdp/current/hive-client/lib/*
   spark.hadoop.metastore.catalog.default hive
   spark.yarn.populateHadoopClasspath false
   ```

2. Copy `/etc/hive/conf/hive-site.xml` into `$SPARK_HOME/conf`. Do not take it from
   `/etc/spark2/conf`: it sets `metastore.catalog.default=spark`, and Hive does not see tables
   created in that catalog.
3. In `$SPARK_HOME/conf/spark-env.sh` (cron starts with an empty environment):

   ```bash
   export HADOOP_CONF_DIR=/etc/hadoop/conf YARN_CONF_DIR=/etc/hadoop/conf
   ```

4. Optional: set `spark.yarn.archive` to an archive of the Spark jars uploaded to HDFS, so the
   jars are not uploaded on every run.

Checks on the first deployment:

1. Run the jobs with `--catalog hive` for a recent day; both finish with exit code 0.
2. In beeline, `SHOW PARTITIONS hdfs_traffic.traffic_minute;` lists the day.
3. In beeline, `SELECT * FROM hdfs_traffic.traffic_minute LIMIT 10;` returns rows (Hive reads
   ORC written by Spark).
4. `SHOW PARTITIONS` on an `fs_*` table lists the new `dt`: the Spark principal may add
   partitions there.

If the Spark client fails on the YARN timeline service, add
`spark.hadoop.yarn.timeline-service.enabled false` to `spark-defaults.conf`.

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

### Daily run

`deploy/daily-report.sh` builds the report for one UTC day: it waits for a NameNode
checkpoint, fetches the image, runs block-mapper, `CompactEvents` and `ResolvePaths`.

Requirements on the host: an HDFS client, GNU `date`, `flock`, `python3`, `curl` with SPNEGO
(`--negotiate`), Spark 3.5.1 in `$SPARK_HOME`. The NameNode checkpoints regularly: the
standby NameNode (HA) or the SecondaryNameNode, with `dfs.namenode.checkpoint.period` of at
most 1 hour.

```
daily-report.sh -n <active NameNode http URL> [-D <dt>] [-r root] [-w workdir]
                [-m mapper.jar] [-j spark-jobs.jar] [-c hive|none]
                [-k keytab -p principal] [-s spark_principal -t spark_keytab]
```

| Option | Default | What it does |
|---|---|---|
| `-n` | required | HTTP URL of the active NameNode, for example `http://<nn_host>:50070` on HDP or `https://<nn_host>:50470` for an HTTPS-only NameNode. With HA find the active one with `hdfs haadmin -getAllServiceState` |
| `-D` | yesterday (UTC) | rerun the given day against the latest image (no `--image-dt`) |
| `-r` | `/data/hdfs-traffic` | `--root` of the jobs |
| `-w` | `/var/tmp/hdfs-traffic` | local working directory: lock, Kerberos ticket, downloaded image |
| `-m` | `/opt/hdfs-traffic/block-mapper.jar` | block-mapper jar |
| `-j` | `/opt/hdfs-traffic/spark-jobs.jar` | spark-jobs jar |
| `-c` | `hive` | `--catalog` of the jobs |
| `-k`, `-p` | none | keytab and principal of the HDFS administrator; both or neither |
| `-s`, `-t` | none | principal and keytab passed to spark-submit as `--principal/--keytab`; required with `-c hive` or `--master yarn` |

Environment variables:

| Variable | Default | What it does |
|---|---|---|
| `SPARK_HOME` | `/opt/spark` | Spark distribution |
| `SPARK_ARGS` | `--master yarn --deploy-mode client` | spark-submit options |
| `POLL_SEC` | 300 | interval between checkpoint checks, seconds |
| `MAX_WAIT_SEC` | 10800 | how long to wait for the checkpoint, seconds |

Steps of a run without `-D`:

1. If `<root>/fsimage/dt=<today>` does not exist, poll `LastCheckpointTime` of the active
   NameNode (JMX `Hadoop:service=NameNode,name=FSNamesystem`) every `POLL_SEC` until it is
   after 00:00 UTC of today. After `MAX_WAIT_SEC` the script exits with code 1 and the
   message
   `daily-report: last checkpoint <time> is before <today> 00:00:00 UTC; is checkpointing (standby NameNode / SecondaryNameNode) healthy?`.
   HTTP 401 or 403 from JMX fails at once: the Kerberos ticket is missing or the user may not
   read JMX.
2. Fetch the image with `hdfs dfsadmin -fetchImage` into `<workdir>/fsimg` and run
   block-mapper into `<root>/fsimage/dt=<today>`.
3. Run `CompactEvents` and `ResolvePaths` for yesterday with `--image-dt <today>`.

A second run while one is going exits with code 0 and a message (`flock` on
`<workdir>/.lock`). The downloaded image is deleted on exit.

Users and permissions:

- The script runs as an HDFS cluster administrator (a member of `dfs.cluster.administrators`,
  for example `hdfs`): `fetchImage` and the mapper need it. `-k/-p` log it in.
- On a secure cluster the Spark jobs cannot run as `hdfs`: YARN's LinuxContainerExecutor
  bans that user. `-s/-t` give them a separate principal. It needs:
  - write access to `<root>/{events,event_path,traffic_minute}`;
  - read access to `<root>/{raw,fsimage}`;
  - write access (or an ACL) on `<root>/fsimage` when the Hive metastore uses storage-based
    authorization: `ResolvePaths` creates the `fs_*` tables and adds partitions there.
- The Spark keytab (`-t`) must be readable by the user cron runs the script as.

Cron at 02:00 UTC, after a post-midnight checkpoint. Cron reads one line per entry, so the
entry is not wrapped. Put `CRON_TZ=UTC` on its own line above it; without it cron uses the
host timezone:

```
CRON_TZ=UTC
0 2 * * * hdfs /opt/hdfs-traffic/daily-report.sh -n http://<nn_host>:50070 -k /etc/security/keytabs/hdfs.headless.keytab -p hdfs-<cluster>@<realm> -s spark-report@<realm> -t /etc/security/keytabs/spark-report.keytab >>/var/log/hdfs-traffic-report.log 2>&1
```

`hdfs-<cluster>@<realm>` is the HDFS headless principal of the cluster (on HDP it is named
after the cluster).

Operations:

- A raw file that arrives after its day was compacted (for example from a DataNode that was
  down at midnight) is not in that day. Rerun the day with `-D <dt>`; both jobs overwrite
  only their own partitions.
- The jobs use local checkpoints, so losing an executor fails the run. Rerun it.
- To replace a stale `fsimage/dt=<today>`, delete it before the run:

  ```bash
  hdfs dfs -rm -r -skipTrash <root>/fsimage/dt=<today>
  ```

- After changing `--root`, move existing tables with `ALTER TABLE ... SET LOCATION`:
  `ADD IF NOT EXISTS PARTITION` keeps the old locations of existing partitions.

Known issues:

- Events are joined to the image by block id only, so federated clusters (several block
  pools) are not supported.
- A path chain deeper than 256 levels, or a cycle (corrupt image), resolves to `?` with a
  WARN in the job log.
- Inodes that exist only in snapshots get `?` (see the block-mapper known issues).

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
