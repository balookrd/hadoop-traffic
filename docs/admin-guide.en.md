# hdfs-traffic-agent: admin guide

[Русская версия](admin-guide.md) · [General description](../README.en.md)

A guide for HDFS administrators: installing the agent on DataNodes, its options, connecting
the metrics to a monitoring system, shipping events into HDFS, running on a large cluster
and the daily reports (block-mapper and spark-jobs). What the dimensions, metrics, event
fields and report tables mean is described in the [README](../README.en.md).

Requirements:

- Apache Hadoop 3.3.x or HDP 3.1 (Hadoop 3.1.1), Java 8;
- for the reports, Spark 3.5.1 (Scala 2.12); Spark 2 is not supported;
- jars built as described in the [README](../README.en.md#build).

Placeholders in this document:

| Placeholder | Meaning |
|---|---|
| `<version>` | project version from `pom.xml`, for example `0.1.0-SNAPSHOT` |
| `<dn_host>`, `<dn_http_port>` | DataNode host and its HTTP port (`dfs.datanode.http.address`) |
| `<nn_host>` | host of the active NameNode |
| `<realm>` | Kerberos realm |
| `<root>` | HDFS directory of the reports, default `/data/hdfs-traffic` |
| `<dt>` | a UTC day, `YYYY-MM-DD` |

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
| `userMetrics` | `false` | `true` publishes per-user counters in JMX (see [Metrics](../README.en.md#metrics)) | turn it on when the per-user series are needed in the metrics system and it can take up to ~60k attributes per DataNode |
| `maxUsers` | 2000 | only with `userMetrics=true`: users that get their own JMX series, first come first served; the rest go to `__other__` | lower it when the metrics sink cannot take ~60k attributes per DataNode |
| `events` | off | path of the active JSONL events file, for example `/var/log/hadoop/hdfs-traffic/events.jsonl`; no commas | set it to enable the events |
| `eventsQueue` | 65536 | events buffered for the writer thread; overflow is dropped and counted in `EventsDropped` | raise it when `EventsDropped` grows |
| `eventsMaxFileMb` | 256 | size at which the active file is rotated, MB | |
| `eventsRotateSec` | 300 | age at which the active file is rotated, seconds | |
| `eventsMaxFiles` | 100 | closed files kept while not shipped; beyond it the oldest are deleted and counted in `EventFilesDropped` | raise it if shipping can stop for longer than the buffer lasts (see [Local buffer on a DataNode](#local-buffer-on-a-datanode)) |
| `flushIntervalSec` | 10 | long operations report their bytes every N seconds (JMX and events); 0 reports them at the end only | |

## Metrics in a monitoring system

This section applies only with `userMetrics=true`: without it the `UserTraffic` bean holds
the four writer thread counters.

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

## Events: local files and shipping into HDFS

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

- exclude `UserTraffic` from the Ambari Metrics sink (see [Metrics in a monitoring system](#metrics-in-a-monitoring-system)), otherwise all
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
`file_block_id` into the current path of the file (see [Reports](../README.en.md#reports-spark-jobs)).

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

## Reports: running the jobs (spark-jobs)

What the jobs do and which tables they write is described in the
[README](../README.en.md#reports-spark-jobs). Build:

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

Each run overwrites only its own `dt` partitions, so a failed run is repeated with the same
`--dt`.

**Warning:** do not run `MSCK REPAIR TABLE` on these tables. The four `fs_*` tables have the
same location, `<root>/fsimage`, and the jobs register each partition at
`<root>/fsimage/dt=<dt>/<table>`. For an image date not yet registered, MSCK would add
`dt=<dt>` at `<root>/fsimage/dt=<dt>`, which holds the files of all four tables, and queries
would read files with other schemas. To undo it, run
`ALTER TABLE hdfs_traffic.<table> DROP PARTITION (dt='<dt>')` on the `fs_*` tables (they are
EXTERNAL, the files stay); the next `ResolvePaths` run that uses that image registers the
partition again with the right location.

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
- The column `event_path.peer_ip` was added to the schema after the first installations.
  `CREATE TABLE IF NOT EXISTS` does not change an existing table, so Hive does not see the
  column there even though new ORC files contain it. Add the column once in beeline:

  ```sql
  ALTER TABLE hdfs_traffic.event_path ADD COLUMNS (`peer_ip` STRING);
  ```

  Partitions written before the `spark-jobs` upgrade have `peer_ip` NULL. To fill a day,
  rerun it with `-D <dt>`.

Known issues:

- Events are joined to the image by block id only, so federated clusters (several block
  pools) are not supported.
- A path chain deeper than 256 levels, or a cycle (corrupt image), resolves to `?` with a
  WARN in the job log.
- Inodes that exist only in snapshots get `?` (see the block-mapper known issues).
