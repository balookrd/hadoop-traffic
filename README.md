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

`__unknown__` means no block token, or a token without a user: Balancer and Mover generate
tokens without a user, so their `replace`/`copy` traffic appears as `__unknown__`.

Counters per series: `Bytes` (payload bytes, no checksums/headers), `EstimatedBytes`
(short-circuit reads: block length per descriptor request — the DataNode cannot see the
actual bytes), `Ops`. Counters are cumulative since DataNode start.

Client-facing traffic = all `role=client` series. `webhdfs_proxy` duplicates the
`webhdfs_*` HTTP traffic on the internal DataTransferProtocol leg; exclude it to avoid
double counting.

## Build

```bash
export JAVA_HOME=/path/to/jdk8   # a full JDK 8 (with lib/tools.jar)
mvn clean verify -P hadoop-3.3.6
mvn clean verify -P hadoop-3.1.1
```

Use `mvn clean verify` (or `mvn clean package`) to produce a distributable jar: a
non-clean build re-shades the already shaded jar.

The jar is `agent/target/hdfs-traffic-agent-<version>.jar` (same jar for both Hadoop lines).

## Install

1. Copy the jar to every DataNode, e.g. `/opt/hdfs-traffic-agent/hdfs-traffic-agent.jar`.
2. Add to `hadoop-env.sh` (HDP: Ambari → HDFS → Configs → Advanced hadoop-env template):

   ```bash
   export HDFS_DATANODE_OPTS="$HDFS_DATANODE_OPTS -javaagent:/opt/hdfs-traffic-agent/hdfs-traffic-agent.jar=maxUsers=2000"
   ```

   `HADOOP_DATANODE_OPTS` (deprecated name) is honored by Hadoop 3 as well.

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

3. Restart DataNodes. The DataNode log shows `hdfs-traffic-agent installed` and
   `hdfs-traffic-agent: instrumented ...` lines. A `WARN ... hook is disabled` line means
   a method was not found in this Hadoop build; other hooks keep working.

Requires `dfs.block.access.token.enable=true` (always the case with Kerberos); without it
every user is `__unknown__`.

## Metrics

JMX bean `Hadoop:service=DataNode,name=UserTraffic`, attributes named
`<Counter>.user.<u>.op.<op>.role.<role>.locality.<loc>` (nntop style), e.g.
`Bytes.user.alice.op.read.role.client.locality.remote`. Also available at
`http://<dn>:<http-port>/jmx?qry=Hadoop:service=DataNode,name=UserTraffic`.
Hadoop's `/jmx` servlet silently drops attributes whose name contains `=`, `:` or a
space, so these characters in a user name are replaced with `_`. A user name may contain
dots: parse the name from the right, as the rule below does.

Cardinality: each user produces up to one series per (op, role, locality) combination
actually seen — typically 8–10 — times 3 counters; at `maxUsers=2000` that is up to
~50–60k attributes per DataNode. The source goes to every metrics2 sink, not only JMX. On
HDP the Ambari Metrics sink (`datanode.sink.timeline`) is enabled by default; to keep
UserTraffic out of it add to `hadoop-metrics2.properties`:

```
datanode.sink.timeline.source.filter.class=org.apache.hadoop.metrics2.filter.GlobFilter
datanode.sink.timeline.source.filter.exclude=UserTraffic
```

Consider a lower `maxUsers` if needed.

Prometheus jmx_exporter rule (with `lowercaseOutputName`, names become e.g.
`hdfs_datanode_user_traffic_bytes_total`):

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

With the `events` option the agent also writes a per-operation event log: one JSON line per
emission, in this field order:

| Field | Meaning |
|---|---|
| `ts` | emission time, ms since the epoch |
| `start` | operation start time, ms |
| `dn` | DataNode host name (resolved once on the writer thread; `unknown` if the lookup fails) |
| `op_id` | `<6 hex>-<n>`, shared by all events of one operation; unique only per DataNode, so an operation across the cluster is identified by the pair (`dn`, `op_id`) |
| `user` | user as in the metrics, but not capped by `maxUsers` |
| `op`, `role`, `locality` | same values as in the metrics |
| `bpid` | block pool id |
| `block_id` | block id; `-1` when not applicable |
| `file_block_id` | block group id for erasure-coded (striped) blocks, otherwise the block id |
| `bytes`, `est_bytes` | delta since the previous event of the same operation: sum over `op_id` to get the total |
| `dur_ms` | time from operation start to this event, ms |
| `last` | `true` closes an operation and counts as one op |
| `peer` | remote address |
| `path` | WebHDFS HTTP-leg events only: the path the DataNode was asked for (e.g. a `._COPYING_` temp name during `hdfs dfs -put`) |

Long operations emit an event every `flushIntervalSec`, so a long-open block shows up as a
series of events rather than one spike at close.

Files: the active file is `events.jsonl` (the name from the `events` option). It is rotated
by size or age into `events-<yyyyMMdd-HHmmss>-<seq>.jsonl`, named by the UTC time of its
first event. The date in the name is the UTC date of its events (`ts`): a file never spans a
UTC day (events reordered by a few milliseconds across midnight stay in the earlier file).
A non-empty active file left over from a crash is rotated when the agent starts (it is
dated by its first event). Only names of that exact pattern are counted and deleted by
`eventsMaxFiles`. The DataNode user must be able to create the events directory (it is created on the first write and
retried on failure).

The DataNode never blocks on the sink: when the queue is full or an I/O error occurs, events
are dropped and counted, with a WARN at most once a minute. Health attributes on the
`UserTraffic` bean: `EventsWritten`, `EventsDropped`, `EventFilesDropped` (counters) and
`EventsQueueSize` (gauge).

Shipping: `deploy/upload-events.sh -d <events dir> -r <HDFS root> [-n <file prefix>]
[-k <keytab> -p <principal>] [-H <host>] [-b <files per put>] [-j <max jitter sec>]` puts
closed files into HDFS as `<root>/dt=YYYY-MM-DD/dn=<host>/<file>` and deletes the local files
only after a successful put. Files go in batches: one `hdfs dfs -mkdir` for all target
directories and one `hdfs dfs -put` per target directory and batch of `-b` files (default
100), so a run starts two or three JVMs instead of one per file; with no closed files it
does nothing (no `kinit`, no JVM). It takes a `flock`, so runs never overlap, and uses its
own Kerberos credential cache in the events directory. The partition `dn=` is the node key:
`hostname -f` by default, `-H` overrides it; it may differ from the `dn` field inside the
events. A failing batch does not stop the run: the other batches are still shipped, the
failed files stay for the next run (a re-put with `-f` is safe: closed files are immutable)
and the exit code is 1. `-j <sec>` sleeps a random 0..sec seconds before starting.

### Running it with a systemd timer

`deploy/systemd/` holds a oneshot service and a timer that starts it every 5 minutes with a
random delay of up to 4 minutes (`RandomizedDelaySec=240`), so the DataNodes of a cluster do
not hit the NameNode and the KDC in the same second. The timer never starts a second run
while one is still going. The service runs as `hdfs` with lowered CPU and I/O priority, a
256 MB client heap and a 30-minute timeout.

On every DataNode, as root:

```bash
install -d /opt/hdfs-traffic-agent
install -m 0755 deploy/upload-events.sh /opt/hdfs-traffic-agent/
install -m 0644 deploy/systemd/hdfs-traffic-upload.service \
    deploy/systemd/hdfs-traffic-upload.timer /etc/systemd/system/
install -m 0644 deploy/systemd/hdfs-traffic-upload.env /etc/default/hdfs-traffic-upload
# edit /etc/default/hdfs-traffic-upload: events dir, HDFS root, keytab and principal
systemctl daemon-reload
systemctl enable --now hdfs-traffic-upload.timer
```

Options of the script go to `UPLOAD_ARGS` in `/etc/default/hdfs-traffic-upload`; `JAVA_HOME`
and `HADOOP_CLIENT_OPTS` can be set there too. The unit looks for `hdfs` in
`/usr/hdp/current/hadoop-client/bin`, `/opt/hadoop/bin` and the system `PATH`; for another
location override `PATH` with `systemctl edit hdfs-traffic-upload.service`.

`RandomizedDelaySec` needs systemd 229 or newer. Older systemd (RHEL/CentOS 7 has 219)
ignores it with a warning in the journal, and all nodes would start together: add `-j 240`
to `UPLOAD_ARGS` there.

Checking:

```bash
systemctl list-timers hdfs-traffic-upload.timer   # next run
systemctl start hdfs-traffic-upload.service       # run now
journalctl -u hdfs-traffic-upload.service         # what was shipped, failures
```

A failed run leaves the service in the `failed` state until the next successful one, so it
can be monitored like any other unit.

Without systemd, cron works too (add `-j` so that nodes do not start together):

```
*/5 * * * * hdfs /opt/hdfs-traffic-agent/upload-events.sh -j 240 -d /var/log/hadoop/hdfs-traffic \
    -r /data/hdfs-traffic/raw -k /etc/security/keytabs/hdfs.headless.keytab -p hdfs@REALM
```

The upload's own writes into HDFS appear as the service user's traffic.

## Block-to-file mapping (block-mapper)

Events carry block ids only. `block-mapper/` turns a downloaded fsimage into the inode
model (block → inode, inode → parent and name) that the Spark jobs use to resolve an event's
`file_block_id` to the file's current path (the resolution is done by the Spark jobs, see
Reports).

Build, once per Hadoop line:

```bash
mvn clean verify -P hadoop-3.1.1      # HDP
mvn clean verify -P hadoop-3.3.6      # Apache Hadoop 3.3.x
```

The jar is `block-mapper/target/block-mapper-<version>-hadoop-<hadoop version>.jar`. Use the
jar built for the cluster's Hadoop line: the generated protobuf classes differ between 3.1
and 3.3. The jar bundles `orc-core` 1.5.13, `orc-shims`, `hive-storage-api`, `aircompressor`,
`commons-lang` 2.6 and `threeten-extra`, all relocated under
`org.hadooptraffic.mapper.shaded` (so Hive jars on `HADOOP_CLASSPATH` cannot shadow them);
`protobuf-java` 2.5.0 comes from Hadoop. Hadoop 3.4+ (where protobuf 2.5 is optional) is not
supported.

Run it on a downloaded image, never on the NameNode's own storage. `-i` is a local file (the
downloaded image), `-o` is any Hadoop FS URI:

```bash
hdfs dfsadmin -fetchImage /data/fsimg      # as an HDFS cluster administrator
hadoop jar block-mapper-0.1.0-SNAPSHOT-hadoop-3.1.1.jar org.hadooptraffic.mapper.FsImageMapper \
  -i /data/fsimg/fsimage_<txid> -o hdfs:///data/hdfs-traffic/fsimage/dt=<YYYY-MM-DD> [--format tsv]
```

`fetchImage` returns the last NameNode checkpoint, not the live namespace: files created
or renamed after it are not in the image (see Reports for how the daily script waits for a
fresh checkpoint). Never call `saveNamespace` on a production NameNode to get a newer one.

Output: one file per table, `<out>/<table>/part-00000.orc`:

| Table | Columns |
|---|---|
| `inode` | `inode_id`, `type` (`F` file, `D` directory, `S` symlink), `name` |
| `edge` | `child_id`, `parent_id` |
| `block_inode` | `block_id`, `inode_id` |
| `meta` | `txid`, `image_file`, `parsed_at` (epoch milliseconds) |

ORC is the default: file version 0.12, ZLIB, bloom filters on `inode_id`, `child_id` and
`block_id`; readable by Hive 3.1 and Spark 3.5. `--format tsv` writes `part-00000.tsv` with
the same columns, tab-separated; `\`, tab, newline and carriage return in names are escaped
as `\\`, `\t`, `\n`, `\r`.

The mapper writes to the hidden sibling `<parent>/.<name>._tmp` (for example
`fsimage/.dt=2026-11-06._tmp`, which Spark and Hive do not take for a partition) and renames
it to `<out>` on success; an existing `<out>` is never overwritten. Exit code 0 is success,
1 a failure, 2 a usage error; messages are prefixed `block-mapper:`. Memory: the namespace
is streamed; snapshot references and the ORC write buffers (bounded by ORC's memory manager)
are held in memory.

Limitations:

- Inodes visible only in snapshots (deleted after a snapshot) have no parent and stay
  unresolved.
- A compressed image saved in parallel (`dfs.image.parallel.save=true` together with
  `dfs.image.compress=true`, Hadoop 3.3) is rejected: fetch an image saved without one of the
  two.

## Reports (spark-jobs)

`spark-jobs/` holds two Spark jobs that turn shipped events and the block-mapper output
into per-minute traffic by user and table. Build (Spark 3.5.1, Scala 2.12; Spark 2 is not
supported):

```bash
mvn clean verify
```

The jar is `spark-jobs/target/spark-jobs-<version>.jar`, the same for both Hadoop lines. On
HDP 3.1 run it with a separate `spark-3.5.1-bin-hadoop3` distribution, not the cluster's
Spark 2.

Both jobs take `--dt YYYY-MM-DD` (required), `--root <dir URI>` (default
`/data/hdfs-traffic`), `--db <name>` (default `hdfs_traffic`) and `--catalog hive|none`
(default `hive`); `ResolvePaths` also takes `--image-dt YYYY-MM-DD`.

- `CompactEvents` reads `<root>/raw/dt=D/dn=*/*.jsonl` and writes `<root>/events/dt=D`.
  Malformed lines are counted and skipped; `*._COPYING_` files of an in-flight `put` are
  not read. A missing raw partition fails the job.
- `ResolvePaths` joins the events of `D` to the fsimage model and writes
  `<root>/event_path/dt=D` (events plus `inode_id`, `table_dir`; `path` is filled in) and
  `<root>/traffic_minute/dt=D`. The image is `--image-dt`, by default the latest
  `fsimage/dt=YYYY-MM-DD` whose `meta/` contains an `.orc` file (hidden temp directories
  are ignored). A file created on `D` appears only in a checkpoint taken after `D` ends,
  so the daily script passes the image of today (`--image-dt <today>`), fetched after a
  post-midnight checkpoint. Paths are current paths as of that checkpoint, with renames
  already applied. WebHDFS events keep the `path` from the event; blocks that are not in
  the image get `?`. When the selected image date is not after `D`, the job logs a WARN:
  files created on `D` may resolve to `?`.

Each run overwrites only its own partitions, so a failed run is simply repeated.

| Table | Columns (all partitioned by `dt`) |
|---|---|
| `events` | the event fields of the JSONL (`ts`, `start`, `dn`, `op_id`, `user`, `op`, `role`, `locality`, `bpid`, `block_id`, `file_block_id`, `bytes`, `est_bytes`, `dur_ms`, `last`, `peer`, `path`) |
| `fs_inode`, `fs_edge`, `fs_block_inode`, `fs_meta` | the block-mapper tables; `dt` is the image date |
| `event_path` | the `events` columns plus `inode_id`, `table_dir` |
| `traffic_minute` | `minute` (epoch ms of the UTC minute start), `user`, `op`, `role`, `locality`, `table_dir`, `bytes`, `est_bytes`, `ops` (number of `last` events) |

All tables are EXTERNAL ORC with ZLIB (`hive/schema.sql`, generated from
`org.hadooptraffic.spark.Ddl`; the jobs create them with `CREATE ... IF NOT EXISTS`).
Partitions are added explicitly by the jobs with `ALTER TABLE ... ADD IF NOT EXISTS
PARTITION`; never run `MSCK REPAIR TABLE`: the four `fs_*` tables share one directory per
image with different schemas. The jobs read the JSONL and ORC files directly, so there is
no Hive table over the raw files.

With `--catalog none` the jobs only write files and register nothing.

### Spark 3.5.1 on HDP 3.1 (YARN, Kerberos, Hive metastore 3.1)

Put this in `$SPARK_HOME/conf/spark-defaults.conf` (not on a one-off command line):

```
spark.sql.hive.metastore.version   3.1.3
spark.sql.hive.metastore.jars      path
spark.sql.hive.metastore.jars.path file:///usr/hdp/current/hive-client/lib/*
spark.hadoop.metastore.catalog.default hive
spark.yarn.populateHadoopClasspath false
```

Also:

- Copy `hive-site.xml` from `/etc/hive/conf` into `$SPARK_HOME/conf`. Do not take it from
  `/etc/spark2/conf`: its `metastore.catalog.default=spark` hides the tables from Hive.
- In `$SPARK_HOME/conf/spark-env.sh` put
  `export HADOOP_CONF_DIR=/etc/hadoop/conf YARN_CONF_DIR=/etc/hadoop/conf` (cron has no
  environment).
- Optionally set `spark.yarn.archive` to a pre-uploaded archive of the Spark jars, so they
  are not shipped on every run.

The YARN / Kerberos / metastore path is not exercised by the stand (it runs Spark with
`--master local` and `--catalog none`). On the first deploy check:

1. One run with `--catalog hive` for a recent day succeeds.
2. `SHOW PARTITIONS hdfs_traffic.traffic_minute;` in beeline lists the day.
3. `SELECT * FROM hdfs_traffic.traffic_minute LIMIT 10;` in beeline returns rows (Hive
   reading ORC written by Spark).
4. `ADD PARTITION` on the `fs_*` tables works for the Spark principal (`SHOW PARTITIONS` on a
   `fs_*` table lists the new `dt` after the run).
5. Possible fix: if the Spark client fails on the YARN timeline service, add
   `spark.hadoop.yarn.timeline-service.enabled false` to `spark-defaults.conf`.

`table_dir` is derived from the path without the Hive metastore: drop the file name, then
cut at the first partition component (`key=value`), service component (starts with `.` or
`_`: `.hive-staging_*`, `_temporary`) or Hive ACID component (`base_N`, `delta_N_N`,
`delete_delta_N_N`, each with an optional `_v<n>` suffix). Staging files that were moved
into the table are attributed to the table; files deleted before the next NameNode
checkpoint (Hive scratch directories, job staging jars, temporary outputs) are not in the
image and give `?`. An unpartitioned table with
subdirectories folds one level lower (to the subdirectory).

Limitations:

- Events are joined to the image by block id only: federated clusters (several block
  pools) are not supported.
- A path chain deeper than 256 levels, or a cyclic one (corrupt image), resolves to `?`
  with a warning in the job log.
- Inodes visible only in snapshots stay `?` (see the block-mapper limitations).
- Tests and the stand bind the Spark driver to `127.0.0.1`, so they do not depend on host
  name resolution.

Example: top tables by write rate for one hour, MB/s per minute:

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

### Daily run

`deploy/daily-report.sh -n <active NameNode http URL> [-D YYYY-MM-DD] [-r root] [-w workdir]
[-m mapper.jar] [-j spark-jobs.jar] [-c hive|none] [-k keytab -p principal] [-s spark principal
-t spark keytab]` needs GNU `date`, `flock`, `python3` and `curl --negotiate`.

`hdfs dfsadmin -fetchImage` returns the last NameNode checkpoint, not the live namespace, and
the script never calls `saveNamespace`. Unless `<root>/fsimage/dt=<today>` already exists, it
first polls the active NameNode's JMX (`LastCheckpointTime`) every `POLL_SEC` (default 300)
for up to `MAX_WAIT_SEC` (default 10800) until a checkpoint after 00:00 UTC of today exists,
and fails with a clear message otherwise. This needs working checkpointing: the standby
NameNode (HA) or the SecondaryNameNode, with `dfs.namenode.checkpoint.period` at most 1 hour.
For HA pass the active NameNode (`hdfs haadmin -getAllServiceState`), for example
`http://nn1.example:50070` on HDP. Then it fetches the image, runs the mapper into
`fsimage/dt=<today>`, and runs `CompactEvents` and `ResolvePaths` for the previous UTC day with
`--image-dt <today>`. To replace a stale `fsimage/dt=<today>`, delete it first. `-D` reruns
a given day against the latest image (no `--image-dt`). A second run while one is active
exits with a message (`flock` on `<workdir>/.lock`); the downloaded image is removed on exit.
`SPARK_ARGS` (default `--master yarn --deploy-mode client`) and `SPARK_HOME` come from the
environment.

Users and permissions:

- The script runs as an HDFS cluster administrator (a member of `dfs.cluster.administrators`,
  for example `hdfs`): `fetchImage` and the mapper need it. `-k/-p` log that user in (both or
  neither).
- The Spark jobs must not run as `hdfs` on a secure cluster (YARN's LinuxContainerExecutor
  bans it). `-s/-t` give spark-submit a separate `--principal/--keytab`; they are required
  with `-c hive` or `--master yarn` and may be omitted for local tests. That user needs
  write access to `<root>/{events,event_path,traffic_minute}` and read access to
  `<root>/{raw,fsimage}`. When the Hive metastore uses storage-based authorization, it also
  needs write access (or an ACL) on `<root>/fsimage`: `ResolvePaths` creates the `fs_*` tables
  and adds partitions there.
- The Spark keytab (`-t`) must be readable by the user running cron.
- For an HTTPS-only NameNode pass `-n https://<nn>:50470`.

Cron, 02:00 UTC (after a post-midnight checkpoint). Cron needs one physical line per entry, so
the entry is not split. `CRON_TZ=UTC` goes on its own line above it; without it the host
timezone applies and 02:00 is host-local time:

```
CRON_TZ=UTC
0 2 * * * hdfs /opt/hdfs-traffic/daily-report.sh -n http://nn1.example:50070 -k /etc/security/keytabs/hdfs.headless.keytab -p hdfs-cluster@REALM -s spark-report@REALM -t /etc/security/keytabs/spark-report.keytab >>/var/log/hdfs-traffic-report.log 2>&1
```

Operations:

- A raw file that arrives after its day was compacted (for example a DataNode that was down
  at midnight) is not in that day: rerun the day with `-D` (both jobs are idempotent per
  partition).
- An executor loss fails the run (the jobs use local checkpoints): rerun it.
- `user` and `start` are reserved words in Hive 3: backquote them in beeline.
- Changing `--root` later needs `ALTER TABLE ... SET LOCATION` for existing tables:
  `ADD IF NOT EXISTS PARTITION` keeps the old locations.

## Docker stand

`stand/` runs the agent on a real secure cluster: MIT KDC, a NameNode, three DataNodes
started with `-javaagent`, and a client container. Kerberos, block access tokens and SASL
data transfer protection are on, as in production. Two distributions are supported: HDP
3.1.0.0-78 (from a local copy, the jars are not redistributable) and Apache Hadoop 3.3.6
(copied out of the `apache/hadoop:3.3.6` image).

```bash
mvn clean package -P hadoop-3.1.1      # JDK 8; the stand mounts agent/target/*.jar
cd stand
./prepare.sh hdp                        # HDP_DIST_DIR, default ~/hdp/3.1.0.0-78
docker compose --env-file .env.hdp up -d --build
./run-scenario.sh hdp
docker compose --env-file .env.hdp down -v
```

For Apache use `apache` and `.env.apache` instead. `run-scenario.sh` drives known traffic,
one user per step: a client write with replication 3, re-replication after `setrep`, remote
and local reads, a WebHDFS write and read. It then checks that all 8 hooked classes are
instrumented and compares the `UserTraffic` delta, summed over the DataNodes' `/jmx`, with
exact expected byte counts. It also ships the events with `upload-events.sh` and checks
them against the same expected byte counts and the WebHDFS paths. DataNode `/jmx` is
published on host ports 29864–29866; the NameNode UI (SPNEGO) is on 29870.

## Limitations

- Write bytes are reported every `flushIntervalSec` while a block is open and the rest on
  close; bytes not yet reported when the DataNode restarts are lost.
- Exact bytes of short-circuit reads are not visible (see `EstimatedBytes`: block length
  once per descriptor request; later reads served from the client's descriptor cache are
  not counted). WebHDFS reads of local replicas may use short-circuit inside the DataNode
  and then appear as `op=scr_fds, role=client` instead of `webhdfs_proxy`.
- Reads: positional reads not aligned to checksum chunks can count up to one chunk (512 B
  by default) more than requested.
- Bytes of a write packet that fails mid-receive are not counted.
- A WebHDFS stream that is never closed loses its event. WebHDFS reads report their bytes
  when the stream closes (no intermediate deltas).
- A race between a late `addBytes` and `finish()` on another thread can lose those bytes,
  in JMX and in events alike.
- DataTransferProtocol locality uses the socket's own addresses (client connected to the
  same IP it came from); a local client using a different local IP of a multi-homed host
  is counted as `remote`.
- Events carry block ids only; file paths come from the fsimage-based mapping
  (block-mapper, see `docs/superpowers/specs/2026-11-06-block-to-file-mapping-design.md`).
