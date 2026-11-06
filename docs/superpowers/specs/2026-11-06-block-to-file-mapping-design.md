# Block-to-file mapping (step 2, stages 1–2) — design

Date: 2026-11-06
Status: draft, awaiting review
Builds on: `2026-11-06-hdfs-user-traffic-design.md` (step 1, JMX accounting).

This document describes how DataNode traffic events are joined with file paths
from the NameNode fsimage to produce per-minute traffic reports in Hive. It is
written for the developers of the agent, `block-mapper` and `spark-jobs`, and
for the reviewers of this design.

## Question answered and stages

The reports answer "who loaded the HDFS network, with which files, and when"
at minute granularity. A DataNode only knows block ids; the block → file
mapping lives in the NameNode. The work is split into two stages:

- Stage 1: the DataNode agent writes per-operation traffic events with block
  ids to local JSONL files; a script run every 5 minutes ships them into HDFS.
- Stage 2: an offline tool parses a downloaded fsimage into an inode model
  (block → inode, inode → parent + name) stored as ORC in HDFS; Spark jobs
  join events with that model and write reports into Hive tables.

Path semantics: a report shows the file's current path as of the fsimage used.
After a rename, bytes written to a staging directory are attributed to the
final location. Files absent from the image (deleted, or visible only in
snapshot diffs) resolve to `?`.

## Supported versions and prerequisites

- Hadoop and Java: same as step 1, Apache Hadoop 3.3.x and HDP 3.1 (Hadoop
  3.1.1), Java 8, Kerberos.
- Spark 3.5.1 / Scala 2.12 on both Hadoop lines. On HDP 3.1 it runs from its
  own `spark-3.5.1-bin-hadoop3` distribution next to the HDP stack. Spark 2 is
  not supported.
- Spark uses the HDP Hive metastore 3.1. Spark cannot write ACID managed
  tables into it, so every table in section 6 is EXTERNAL.

## 1. Project layout

The root `pom.xml` is a parent with the shared profiles `hadoop-3.1.1` and
`hadoop-3.3.6`. In the table, `<ver>` is the project version and
`<hadoop_version>` is the Hadoop version of the build profile.

| Module | Content | Artifact |
|---|---|---|
| `agent/` | step 1 code, moved with `git mv` | `hdfs-traffic-agent-<ver>.jar`, one jar for both Hadoop lines |
| `block-mapper/` | fsimage → ORC tool | `block-mapper-<ver>-hadoop-<hadoop_version>.jar`, one per profile |
| `spark-jobs/` | `CompactEvents`, `ResolvePaths` | `spark-jobs-<ver>.jar`, one jar (Spark 3.5.1 `provided`) |

`block-mapper` is built per profile because the generated `FsImageProto`
classes reference `com.google.protobuf` in Hadoop 3.1 and
`org.apache.hadoop.thirdparty.protobuf` in 3.3. `spark-jobs` depends only on
Spark 3.5.1, which carries its own Hadoop client, so one artifact serves both
Hadoop lines. Stand scripts and README paths point to `agent/target/`.

## 2. Agent: event model changes

### 2.1 Deltas for long operations

`OpContext` gets the fields `long opSeq`, `lastEmitNanos`, `emittedBytes`,
`emittedEstimatedBytes`. On every `addBytes` it compares the clock with
`lastEmitNanos`. When `flushIntervalSec` has passed, it emits an intermediate
event with only the bytes accumulated since the previous emission. `finish()`
emits the remainder with `last = true`, also when the remainder is zero, so
every operation ends with exactly one `last` event.

Emission runs in a short `synchronized` block on the context. For
DataTransferProtocol the lock is uncontended; WebHDFS contexts can be touched
by several threads. The clock is injectable for tests.

The operation id string `"<6 hex>-<n>"` is not built on the data path. The
context holds only `opSeq`, and `TrafficEvent.getOpId()` builds the string on
the writer thread. The 6-hex prefix is random per process, so `op_id` is
unique per DataNode only; across the cluster an operation is identified by
the pair (`dn`, `op_id`).

### 2.2 `TrafficEvent` fields

New fields:

| Field | Type | Content |
|---|---|---|
| `opSeq` | long | operation sequence number; `getOpId()` derives the string from a per-process random prefix and this counter, unique per DataNode across restarts |
| `startMillis` | long | operation start |
| `last` | boolean | `true` on the final event of an operation |
| `path` | String | WebHDFS HTTP-leg events only, otherwise null |

`bytes` and `estimatedBytes` now hold the delta since the previous event of
the same operation.

`fileBlockId` is derived: for striped (negative) ids it is `blockId & ~0xF`,
the block group id stored in the fsimage; otherwise it equals `blockId`.

### 2.3 JMX aggregator

Bytes are added on every event; `Ops` is incremented only on `last = true`.
Totals do not change; counters of long writes now grow during the write
instead of jumping at block close. The README limitation about long-open
blocks is reworded accordingly.

### 2.4 WebHDFS path

The `WebHdfsHandler` request advice from step 1 passes the handler (`@This`)
to the hook. The hook reads the handler's private `path` field by reflection
into the pending context; the field lookup is cached for the handler class (a
single entry). The field exists in Hadoop 3.1.1 and 3.3.6. Any failure
disables only path capture and logs one WARN; WebHDFS accounting continues.
`@Advice.FieldValue` is not used because a missing field would disable the
whole advice.

## 3. Agent: JSONL sink

### 3.1 Options

The options are added to the `-javaagent` option string. As in step 1, an
unknown or invalid option disables the agent.

| Parameter | Default | What it does | When to change |
|---|---|---|---|
| `events` | not set | path of the active events file, e.g. `/var/log/hadoop/hdfs-traffic/events.jsonl`; without it no sink is created | set it to turn the JSONL sink on |
| `eventsQueue` | 65536 | queue capacity, in events | when `EventsDropped` grows because the queue is full |
| `eventsMaxFileMb` | 256 | rotate when the active file reaches this size, MB | to make closed files smaller or larger |
| `eventsRotateSec` | 300 | rotate when the active file is this old and non-empty, seconds | to change how long events wait in the active file before they can be shipped |
| `eventsMaxFiles` | 100 | maximum number of closed, not yet shipped files kept locally | when `EventFilesDropped` grows because shipping falls behind |
| `flushIntervalSec` | 10 | interval of delta events (section 2.1), seconds; `0` disables intermediate events; applies to all sinks, including JMX | to trade event count for freshness of long operations |

### 3.2 Writer behaviour

- `JsonlEventSink.onEvent` only calls `ArrayBlockingQueue.offer()`. On a full
  queue the event is dropped and `EventsDropped` grows. The data path does no
  I/O, no blocking waits and no formatting; `offer` takes a short lock.
- The daemon thread `hdfs-traffic-events` drains the queue in batches, formats
  JSON by hand (no Jackson: its versions differ between Hadoop lines), writes
  through a `BufferedWriter` and flushes at least once a second.
- Rotation happens on size, on age, or on a UTC day boundary. It closes the
  active file and renames it to `events-<yyyyMMdd-HHmmss>-<seq>.jsonl` in the
  same directory, where `<yyyyMMdd-HHmmss>` is the UTC time of the file's
  first event and `<seq>` is a six-digit sequence number. Closed files are not
  modified again.
- The day boundary is decided per event by its `ts`: an event of a later UTC
  day closes the current file first. The day only moves forward, so an event
  reordered by a few milliseconds across midnight stays in the earlier file. A
  file never holds events of two UTC days, so the date in its name is the date
  of all its events.
- A non-empty active file found when the writer starts (left after a crash)
  is rotated at writer start, not on the first write. It is dated by the `ts`
  of its first event, or by its mtime if the first line is unreadable.
- When more than `eventsMaxFiles` closed files exist, the oldest are deleted
  and `EventFilesDropped` grows.
- Formatting failures are per event: a `RuntimeException` from the formatter
  drops and counts only that event; the rest of the batch is written.
- I/O errors: the batch's events are dropped and counted, a WARN is logged at
  most once a minute, and the file is reopened on the next batch. A missing or
  unwritable directory at startup does not fail the DataNode; the sink keeps
  retrying.
- The shutdown hook drains the queue for at most 2 s, then closes and rotates
  the active file.
- `user` is written as is, without the `maxUsers` cap; that cap limits JMX
  cardinality only.
- JMX: `UserTraffic` gets the global attributes `EventsWritten`,
  `EventsDropped`, `EventsQueueSize`, `EventFilesDropped`.

### 3.3 Line format

```json
{"ts":1762430000123,"start":1762429990000,"dn":"dn1.example","op_id":"7f3a9c-000123",
 "user":"alice","op":"read","role":"client","locality":"remote",
 "bpid":"BP-1-10.0.0.1-1700000000000","block_id":1073741901,"file_block_id":1073741901,
 "bytes":67108864,"est_bytes":0,"dur_ms":10123,"last":false,
 "peer":"/10.0.0.5:51234","path":null}
```

| Field | Content |
|---|---|
| `ts` | emission time, epoch ms |
| `start` | operation start, epoch ms |
| `dn` | DataNode host name, resolved once on the writer thread, never in premain; `unknown` if the lookup fails |
| `block_id` | `-1` when the operation has no block |

## 4. Shipping events to HDFS

`deploy/upload-events.sh` runs on every DataNode every 5 minutes, as the user
that owns the events directory. The recommended runner is the systemd timer in
`deploy/systemd/`: a oneshot service as user `hdfs` and a timer
(`OnCalendar=*:0/5`) with `RandomizedDelaySec=240`, so the DataNodes do not hit
the NameNode and the KDC in the same second. systemd older than 229 (RHEL /
CentOS 7) ignores `RandomizedDelaySec`; there, and under cron, pass `-j 240`.

`<root>` below is the HDFS root of the report data, default
`/data/hdfs-traffic`.

| Parameter | Default | What it does | When to change |
|---|---|---|---|
| `-d` | `/var/log/hadoop/hdfs-traffic` | local events directory (directory of the agent's `events=` file) | when `events=` points elsewhere |
| `-r` | `/data/hdfs-traffic/raw` | HDFS root of raw files | with a different `<root>` |
| `-n` | `events` | file name prefix | when the `events=` file has a different base name |
| `-k`, `-p` | not set | keytab and principal for `kinit`; `-k` requires `-p` | on a Kerberos cluster |
| `-H` | `hostname -f` | value of the `dn=` partition | when `hostname -f` does not give the node name you want in the partition |
| `-b` | 100 | files per `hdfs dfs -put` | — |
| `-j` | 0 | sleep a random 0..N seconds before the run | under cron or on systemd < 229 |

One run:

1. Take a `flock` on `<events_dir>/.upload.lock`, where `<events_dir>` is the
   `-d` directory. If another run holds it, exit 0.
2. List the closed files `events-*.jsonl`, sorted by name, which is UTC time.
   If there are none, exit without `kinit` and without starting a JVM.
3. If `-k` is set, run `kinit -kt <keytab> <principal>`.
4. Create all target directories
   `<root>/raw/dt=<dt>/dn=<host>/` with one `hdfs dfs -mkdir -p`. Here `<dt>`
   is the UTC date from the file name in `YYYY-MM-DD` form and `<host>` is the
   `-H` value.
5. For each target directory, put the files in batches of `-b` with
   `hdfs dfs -put -f`, then delete the local files of the batch. A failed
   batch stays local for the next run; the other batches are still shipped
   and the script exits 1.

The `dn=` partition is the shipper's host name and may differ from the `dn`
field inside the events. Re-sending after a failure is idempotent: the same
file name is put again with `-f`. Upload traffic itself appears in the reports
as writes of the service user.

## 5. block-mapper

### 5.1 Usage

```bash
hdfs dfsadmin -fetchImage /data/fsimg
hadoop jar block-mapper-<ver>-hadoop-<hadoop_version>.jar org.hadooptraffic.mapper.FsImageMapper \
  -i /data/fsimg/fsimage_<txid> -o hdfs:///data/hdfs-traffic/fsimage/dt=<dt> [--format orc|tsv]
```

`<txid>` is the transaction id in the downloaded image's file name.

The mapper always runs on a downloaded image, never against the active
NameNode.

| Parameter | Value | What it does |
|---|---|---|
| `-i` | local file | downloaded fsimage |
| `-o` | any Hadoop FS URI with a parent directory | output directory `<out>` |
| `--format` | `orc` (default) or `tsv` | output format, see 5.3 |

An existing `<out>` is never overwritten (exit 1). Exit codes: 0 success, 1
failure, 2 usage error.

### 5.2 Parsing

1. Read `FileSummary`; its length is in the last 4 bytes of the file. It gives
   the section offsets and the compression codec (`dfs.image.compress`).
   Images saved in parallel (`dfs.image.parallel.save`) are read through their
   parent sections; a compressed parallel image is rejected.
2. Load `INODE_REFERENCE` into a map `refId → referredId, hasName` before
   `INODE_DIR`; the section is small.
3. Stream the needed sections by offset:
   - `INODE` → `inode` rows (`inode_id`, `type` F/D/S, `name`) and, for files,
     one `block_inode` row per block (`block_id`, `inode_id`). Striped files
     store block group ids, which match `file_block_id` from the events.
   - `INODE_DIR` → `edge` rows (`child_id`, `parent_id`) from `children`. For
     `refChildren` the reference map is consulted: a reference without a name
     (`DstReference`, the current location) yields an edge to `referredId`; a
     named reference (`WithName`, a snapshot view) is skipped.
4. Write `meta`: image txid, image file name, parse time.

Memory holds the snapshot reference map and the ORC write buffers, which ORC's
memory manager bounds. The directory tree is never built in memory.

### 5.3 Output

ORC (default) is written directly to the target URI with `orc-core` 1.5.13,
the ORC line of Hive 3.1, through `VectorizedRowBatch`:

- ZLIB compression, file version 0.12, only `bigint` and `string` columns;
- bloom filters on the join keys `inode_id`, `child_id`, `block_id`;
- `meta.parsed_at` in epoch milliseconds;
- readable by Hive 3.1 on HDP and by Spark 3.5.1.

`orc-core` 1.5.13, `orc-shims`, `hive-storage-api`, `aircompressor`,
`commons-lang` 2.6 and `threeten-extra` are bundled into the mapper jar and
relocated under `org.hadooptraffic.mapper.shaded`, including their
`META-INF/services` entries. Reason: `hadoop jar` loads classes parent-first,
so Hive jars on `HADOOP_CLASSPATH` would otherwise replace these classes one by
one. `protobuf-java` 2.5.0 comes from Hadoop; Hadoop 3.4+, where protobuf 2.5
is optional, is not supported.

`--format tsv` writes tab-separated text with escaped names, for tests and
manual inspection.

Output layout: `<out>/{inode,edge,block_inode,meta}/part-00000.<ext>`, where
`<ext>` is `orc` or `tsv`. The mapper writes into the hidden sibling
`<parent>/.<name>._tmp`, where `<parent>` and `<name>` are the parent and last
component of `<out>` (e.g. `fsimage/.dt=2026-11-06._tmp`). The hidden name
keeps Spark and Hive from taking the directory for a `dt=` partition. On
success the directory is renamed to `<out>`; on failure the tool exits
non-zero with an error message and leaves no partial `<out>`.

## 6. Hive tables and Spark jobs

### 6.1 Tables (`hive/schema.sql`)

| Table | Format | Partition | Content |
|---|---|---|---|
| `events` | ORC | `dt` | compacted events, typed |
| `fs_inode`, `fs_edge`, `fs_block_inode`, `fs_meta` | ORC | `dt` (image date) | mapper output |
| `event_path` | ORC | `dt` | events + `inode_id`, `table_dir`; `path` is filled in |
| `traffic_minute` | ORC | `dt` | `minute, user, op, role, locality, table_dir, bytes, est_bytes, ops`; `minute` is the epoch ms of the UTC minute start |

- All tables are EXTERNAL and ORC with ZLIB compression; Spark writes them
  with `compression=zlib`.
- Database name and root directory are parameters.
- There is no Hive table over the raw JSONL: the jobs read the files directly.
  A Hive JSON table would need the HCatalog SerDe.
- The DDL source of truth is `org.hadooptraffic.spark.Ddl`; `hive/schema.sql`
  is generated from it.

Every mapper run is registered in each `fs_*` table explicitly, one partition
per table, by `ResolvePaths` before it reads:

```sql
ALTER TABLE fs_<model_table> ADD IF NOT EXISTS PARTITION (dt='<image_dt>')
  LOCATION '<root>/fsimage/dt=<image_dt>/<model_table>'
```

`<model_table>` is one of `inode`, `edge`, `block_inode`, `meta`; `<image_dt>`
is the image date. **Warning:** do not run `MSCK REPAIR TABLE` on the `fs_*`
tables. The mapper's `<out>` holds four tables with different schemas, which
`MSCK REPAIR TABLE` cannot map to the four `fs_*` tables.

### 6.2 `CompactEvents --dt <dt>`

- Reads the files `raw/dt=<dt>/dn=*/*.jsonl` directly, casts them to the typed
  schema and writes `events/dt=<dt>`, overwriting only that partition.
- Malformed lines are counted and skipped.
- Only `*.jsonl` files are read, because an in-flight `hdfs dfs -put` leaves
  `._COPYING_` files in the partition.
- If no matching file exists, the job fails.

### 6.3 `ResolvePaths --dt <dt> [--image-dt <image_dt>]`

1. Image: `<image_dt>`; by default the latest `dt=` directory under
   `<root>/fsimage/` whose `meta/` contains an `*.orc` file. This rule accepts
   Spark-written test data and block-mapper output; hidden temp directories
   are skipped by name. For current-path semantics the image must be from
   `<dt>+1`: files created during `<dt>` are present in a NameNode checkpoint
   taken after midnight UTC. `fetchImage` returns the last checkpoint, not the
   live namespace. A WARN is logged when the image date is not after `<dt>`.
2. Distinct `file_block_id` of `events/dt=<dt>` → `fs_block_inode` → the
   inode ids needed.
3. Ancestor closure: climb level by level from the needed inodes, joining the
   frontier with `fs_edge` until only the root is left. `fs_edge` is
   deduplicated, persisted and repartitioned by `child_id`, not broadcast.
   One iteration per tree level, capped at 256 with a warning. The closure is
   small compared with the namespace.
4. Paths to the root by pointer jumping inside the closure: each iteration
   joins a node with its current ancestor, prepends that ancestor's name chain
   and doubles the step, about log₂(depth) iterations.
5. Names from `fs_inode` are concatenated root-first into `path`.
6. `event_path/dt=<dt>`: every event with `inode_id` and `path`. WebHDFS
   HTTP-leg events take `path` from the event; unresolved blocks get `?`.
7. `traffic_minute/dt=<dt>`: grouped by minute of `ts` and by `table_dir`;
   `ops` is the count of `last` events.

### 6.4 `table_dir` heuristic

Tables do not sit at a fixed depth (`/data/warehouse/db/t` and
`/data/warehouse/x/db/t` both occur), so `table_dir` is derived from the file
path by a heuristic, without the Hive metastore (user decision):

- drop the file name;
- cut the directory path at the first component that is one of:
  - a partition (`key=value`);
  - a service directory, i.e. any name starting with `.` or `_`
    (`.hive-staging_*`, `.spark-staging-*`, `_temporary`, …);
  - a Hive ACID directory: `base_<n>`, `delta_<n>_<n>[_<n>]`,
    `delete_delta_<n>_<n>[_<n>]`, each with an optional `_v<n>` suffix, where
    `<n>` is a number;

  everything from that component on is dropped, including `-ext-10000`,
  attempt and bucket directories inside staging;
- the rest is `table_dir`; `?` stays `?`.

Staging files:

- A file written into `<table>/.hive-staging_*/…` or `<table>/_temporary/…`,
  where `<table>` is the table directory, keeps its inode and block ids when
  the job renames it into place. With an image taken after the rename the path
  is already the final one. With an image taken while the job still ran, the
  cut at the staging component still yields `<table>`.
- Files deleted before the next checkpoint (staging files of a failed job,
  Hive scratch directories, job staging jars, temporary outputs) are absent
  from the image and resolve to `?`.
- Renames move no data, so no bytes are counted twice.

Known limit: an unpartitioned table whose files sit in plain subdirectories
gets the subdirectory as `table_dir`, one level below the table.

### 6.5 Daily run

`deploy/daily-report.sh` runs from cron at 02:00 UTC, after a post-midnight
checkpoint:

1. Unless `fsimage/dt=<today>` already exists, wait for a checkpoint: poll the
   active NameNode's `LastCheckpointTime` (JMX) every `POLL_SEC` seconds
   (environment variable, default 300) until it is after 00:00 UTC of today.
   After `MAX_WAIT_SEC` seconds (default 10800) the script exits with an
   error.
2. Fetch the image and run the mapper into `fsimage/dt=<today>`. Step 1 and
   this step are skipped when `fsimage/dt=<today>` exists.
3. Run `CompactEvents` and `ResolvePaths` for the previous day with
   `--image-dt <today>`.

The script never calls `saveNamespace`: in production it would put the active
NameNode into safe mode and block it.

`-D <dt>` reruns a given day against the latest image.

## 7. Failure handling

- The sink does not block or fail the DataNode: the queue is bounded, drops
  are counted, I/O errors are retried.
- Lost delivery windows show up as `EventsDropped` and `EventFilesDropped`;
  totals can be cross-checked against JMX.
- The mapper and the Spark jobs can be re-run per partition.

## 8. Tests

- Agent unit: delta emission with an injected clock, `last` and `Ops` in the
  aggregator, `fileBlockId` normalization, JSON escaping, rotation by size and
  by age, file naming, `eventsMaxFiles` cleanup, drops on a full queue,
  option parsing.
- Agent IT (existing `MiniDFSCluster` tests, plus `events=`): per user, the sum
  of `bytes` in event files equals JMX `Bytes`, and the number of `last`
  events equals `Ops`; WebHDFS events carry `path`.
- Mapper IT: `MiniDFSCluster` with directories, files, a directory rename, a
  snapshot followed by a rename out of the snapshotted directory, and an EC
  file (`XOR-2-1`, 3 DataNodes). The test runs `saveNamespace`, then the
  mapper, and checks for every block that the path assembled from the output
  equals the path from the NameNode API. Both profiles, both formats.
  `FsImageReaderIT` and `FsImageMapperIT` share `ImageFixture` (directory
  rename, snapshot + rename out, delete after a snapshot, `XOR-2-1-1024k` EC
  file).
- Spark jobs: JUnit with a local `SparkSession` on synthetic ORC inputs in the
  block-mapper schemas, and an embedded Derby metastore for `HiveCatalogIT`.
  Compatibility of mapper files with Spark is checked on the stand. Cases:
  pointer jumping, `?`, WebHDFS path, `table_dir` heuristic (partitions at
  varying depth, staging and `_temporary` directories, unpartitioned tables),
  `ops` counting.
- Stand (`stand/run-scenario.sh`, warehouse step, runs on `tr-nn`):
  - alice writes into `.hive-staging` and moves the file out to the table;
    bob writes one level deeper (`/warehouse/extra/db2/t2`); carol's file
    stays in staging;
  - events are shipped;
  - `safemode enter` / `saveNamespace` / `safemode leave` forces a checkpoint
    before `fetchImage`, because `fetchImage` returns the last checkpoint.
    This sequence is for the stand only and is forbidden in production;
  - the mapper and `spark-submit --master local[2]` (`--catalog none`) run;
  - `check_traffic.py tables` checks the bytes by `table_dir` on `event_path`.

  The stand does not cover the YARN, Kerberos and Hive metastore path.

## Out of scope

- Edit log tailing for fresher mappings (stage 3).
- Kafka, ClickHouse, real-time dashboards.
- Path as of the time of the operation.
- NameNode-side agent.
- Workflow orchestration (Oozie, Airflow); only cron examples are provided.
