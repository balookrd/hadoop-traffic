# Block-to-file mapping (step 2, stages 1–2) — design

Date: 2026-11-06
Status: draft, awaiting review
Builds on: `2026-11-06-hdfs-user-traffic-design.md` (step 1, JMX accounting).

## Goal

Answer "who loaded the HDFS network, with which files, and when" at minute
granularity. A DataNode only knows block ids; the block → file mapping lives in
the NameNode. So:

- **Stage 1** — the DataNode agent writes per-operation traffic events (with
  block ids) to local JSONL files; a cron job ships them into HDFS.
- **Stage 2** — an offline tool parses a downloaded fsimage into an inode
  model (block → inode, inode → parent + name) stored as ORC in HDFS;
  Spark jobs join events with that model and build reports in Hive tables.

Path semantics: the report shows the file's **current** path as of the fsimage
used (after a rename, bytes written to a staging directory are attributed to
the final location). Files absent from the image (deleted, or visible only in
snapshot diffs) resolve to `?`.

## Environment

Same as step 1: Apache Hadoop 3.3.x and HDP 3.1 (Hadoop 3.1.1), Java 8,
Kerberos. Spark: 3.5.1 / Scala 2.12 on both lines (on HDP 3.1 it runs from
its own `spark-3.5.1-bin-hadoop3` distribution next to the HDP stack); Spark 2
is not supported. Spark talks to the HDP Hive metastore 3.1, which it cannot
write ACID managed tables into, so every table in §6 is EXTERNAL.

## 1. Project layout

The root `pom.xml` becomes a parent with the shared profiles `hadoop-3.1.1`
and `hadoop-3.3.6`. Modules:

| Module | Content | Artifact |
|---|---|---|
| `agent/` | current code, moved with `git mv` | `hdfs-traffic-agent-<ver>.jar`, one jar for both Hadoop lines |
| `block-mapper/` | fsimage → ORC tool | `block-mapper-<ver>-hadoop-<x>.jar`, per profile |
| `spark-jobs/` | `CompactEvents`, `ResolvePaths` | `spark-jobs-<ver>.jar`, one jar (Spark 3.5.1 `provided`) |

`block-mapper` is built per profile because the generated `FsImageProto`
classes reference `com.google.protobuf` in 3.1 and
`org.apache.hadoop.thirdparty.protobuf` in 3.3. `spark-jobs` depends only on
Spark 3.5.1 (which carries its own Hadoop client), so it is one artifact for
both Hadoop lines.
Stand scripts and README paths are updated to `agent/target/`.

## 2. Agent: event model changes

### 2.1 Deltas for long operations

`OpContext` gains a `long opSeq`, `lastEmitNanos`, `emittedBytes`,
`emittedEstimatedBytes`. On every `addBytes` it compares the clock with
`lastEmitNanos`; when `flushIntervalSec` (default 10, `0` disables) has passed
it emits an intermediate event carrying only the bytes accumulated since the
previous emission. `finish()` emits the remainder with `last = true`
(also when the remainder is zero, so every operation ends with exactly one
`last` event).

Emission is guarded by a short `synchronized` block on the context
(uncontended for DataTransferProtocol; WebHDFS contexts can be touched by
several threads). The clock is injectable for tests.

The `"<6 hex>-<n>"` operation id string is not built on the data path: the
context only holds `opSeq`, and `TrafficEvent.getOpId()` builds the string
lazily on the writer thread. `op_id` is unique per DataNode only (the prefix
is random per process); an operation across the cluster is identified by the
pair (`dn`, `op_id`).

### 2.2 `TrafficEvent`

New fields: `opSeq` (long; `getOpId()` derives the string "per-process random prefix
and counter", unique per DataNode across restarts), `startMillis`, `last` (boolean),
`path` (String, WebHDFS HTTP-leg events only, otherwise null). `bytes` / `estimatedBytes` now mean the delta
since the previous event of the same operation.

`fileBlockId` is derived: for striped (negative) ids `blockId & ~0xF`
(the block group id stored in the fsimage), otherwise `blockId`.

### 2.3 JMX aggregator

Bytes are added on every event; `Ops` is incremented only on `last = true`.
Totals are unchanged; long writes now grow gradually instead of jumping at
block close. The README limitation about long-open blocks is reworded.

### 2.4 WebHDFS path

The existing `WebHdfsHandler` request advice passes the handler (`@This`) to
the hook, which reads its private `path` field reflectively (lookup cached for
the handler class, a single entry) into the pending context. The field exists in 3.1.1 and 3.3.6; any
failure disables only path capture (one WARN), not WebHDFS accounting —
unlike `@Advice.FieldValue`, which would disable the whole advice.

## 3. Agent: JSONL sink

### 3.1 Options

Added to the `-javaagent` option string (unknown or invalid options still
disable the agent, as today):

| Option | Default | Meaning |
|---|---|---|
| `events` | — | active file path, e.g. `/var/log/hadoop/hdfs-traffic/events.jsonl`; without it no sink is created |
| `eventsQueue` | 65536 | queue capacity, events |
| `eventsMaxFileMb` | 256 | rotate when the active file reaches this size |
| `eventsRotateSec` | 300 | rotate when the active file is this old (and non-empty) |
| `eventsMaxFiles` | 100 | max closed, not yet shipped files kept locally |
| `flushIntervalSec` | 10 | delta emission interval (section 2.1); applies to all sinks, including JMX |

### 3.2 Behaviour

- `JsonlEventSink.onEvent` only calls `ArrayBlockingQueue.offer()`; on a full
  queue the event is dropped and `EventsDropped` grows. No I/O, no blocking
  waits and no formatting on the data path (`offer` takes a short lock).
- A daemon thread `hdfs-traffic-events` drains the queue in batches, formats
  JSON by hand (no Jackson: versions differ between Hadoop lines), writes
  through a `BufferedWriter`, flushes at least once a second.
- Rotation (size, age, or a UTC day boundary) closes the active file and
  renames it to the immutable `events-<yyyyMMdd-HHmmss>-<seq>.jsonl` in the
  same directory, named after the UTC time of the file's first event. The day
  boundary is decided per event by its `ts`: an event of a later UTC day closes
  the current file first; the day only moves forward, so an event reordered by
  a few milliseconds across midnight stays in the earlier file. A file never
  spans a UTC day of its events, so the date in its name is the date of all its
  events. A non-empty active file found when the writer starts (crash leftover)
  is rotated right then, not on the first write; it is dated by the `ts` of its
  first event (its mtime if the first line is unreadable).
  When more than `eventsMaxFiles` closed files exist, the oldest are deleted
  and `EventFilesDropped` grows.
- Formatting failures are per event: a `RuntimeException` from the formatter
  drops (and counts) only that event, the rest of the batch is written.
- I/O errors: events are dropped with the counter, a WARN is logged at most
  once a minute, the file is reopened on the next batch. A missing or
  unwritable directory at startup does not fail the DataNode; the sink keeps
  retrying.
- Shutdown hook drains the queue for at most 2 s, then closes and rotates the
  active file.
- `user` is written as-is, without the `maxUsers` cap (that cap protects JMX
  cardinality only).
- JMX: `UserTraffic` gains global attributes `EventsWritten`, `EventsDropped`,
  `EventsQueueSize`, `EventFilesDropped`.

### 3.3 Line format

```json
{"ts":1762430000123,"start":1762429990000,"dn":"dn1.example","op_id":"7f3a9c-000123",
 "user":"alice","op":"read","role":"client","locality":"remote",
 "bpid":"BP-1-10.0.0.1-1700000000000","block_id":1073741901,"file_block_id":1073741901,
 "bytes":67108864,"est_bytes":0,"dur_ms":10123,"last":false,
 "peer":"/10.0.0.5:51234","path":null}
```

`ts` — emission time (ms), `start` — operation start, `dn` — DataNode
hostname resolved once on the writer thread (never in premain; `unknown` if the
lookup fails), `block_id = -1` when not applicable.

## 4. Shipping events to HDFS

`deploy/upload-events.sh`, run by cron on every DataNode every 5 minutes:

1. `kinit -kt <keytab> <principal>` (service user, configurable).
2. For each closed `events-*.jsonl`:
   `hdfs dfs -put -f <file> /data/hdfs-traffic/raw/dt=<date from name>/dn=<host>/`,
   then delete the local file. The `dn=` partition is the shipper's host name
   (`hostname -f`, `-H` overrides) and may differ from the `dn` field inside
   the events. One failed file does not stop the run: the others are still
   shipped and the script exits non-zero.

Re-sending after a failure is idempotent (same name, `-f`). Upload traffic is
itself visible in reports as the service user's writes. Root directory,
keytab and principal are script parameters.

## 5. block-mapper

### 5.1 Usage

```bash
hdfs dfsadmin -fetchImage /data/fsimg
hadoop jar block-mapper-<ver>-hadoop-<x>.jar org.hadooptraffic.mapper.FsImageMapper \
  -i /data/fsimg/fsimage_<txid> -o hdfs:///data/hdfs-traffic/fsimage/dt=<date> [--format orc|tsv]
```

Always runs on a downloaded image, never against the active NameNode. `-i` is a local file
(the downloaded image); `-o` is any Hadoop FS URI. `-o` must have a parent directory.

An existing `<out>` is never overwritten (exit 1). Exit codes: 0 success, 1
failure, 2 usage error.

### 5.2 Parsing

1. Read `FileSummary` (its length is in the last 4 bytes of the file): section
   offsets and the compression codec (`dfs.image.compress`).
   Images saved in parallel (`dfs.image.parallel.save`) are read through their
   parent sections; a compressed parallel image is rejected.
2. Stream the needed sections by offset:
   - `INODE` → `inode` rows (`inode_id`, `type` F/D/S, `name`) and, for files,
     one `block_inode` row per block (`block_id`, `inode_id`). Striped files
     store block group ids, matching `file_block_id` from events.
   - `INODE_DIR` → `edge` rows (`child_id`, `parent_id`) from `children`; for
     `refChildren`, look up `INODE_REFERENCE`: a reference without a name
     (`DstReference`, the current location) yields an edge to `referredId`;
     a named reference (`WithName`, a snapshot view) is skipped.
   - `INODE_REFERENCE` is loaded into a map (`refId → referredId, hasName`)
     before `INODE_DIR`; it is small.
3. `meta`: image txid, image file name, parse time.

Memory is the snapshot references plus the ORC write buffers (bounded by ORC's memory
manager); the tree is never built in memory.

### 5.3 Output

ORC (default) written directly to the target URI with `orc-core` 1.5.13 (the
ORC line of Hive 3.1) through `VectorizedRowBatch`: ZLIB compression, file
version 0.12, only `bigint` / `string` columns, bloom filters on the join keys
(`inode_id`, `child_id`, `block_id`). Readable by Hive 3.1 on HDP and by
Spark 3.5.1. `orc-core` 1.5.13, `orc-shims`, `hive-storage-api`, `aircompressor`,
`commons-lang` 2.6 and `threeten-extra` are bundled into the mapper jar and relocated under
`org.hadooptraffic.mapper.shaded` (`hadoop jar` is parent-first, so Hive jars on
`HADOOP_CLASSPATH` must not win class by class; `META-INF/services` entries are relocated
too). `protobuf-java` 2.5.0 comes from Hadoop; Hadoop 3.4+ (where protobuf 2.5 is optional)
is not supported. `meta.parsed_at` is epoch milliseconds. `--format tsv` writes
tab-separated text (names escaped) for tests and manual inspection.

Output layout: `<out>/{inode,edge,block_inode,meta}/part-00000.<ext>`.
Writing goes to the hidden sibling `<parent>/.<name>._tmp` (e.g.
`fsimage/.dt=2026-11-06._tmp`, never a name that looks like a `dt=` partition to Spark or
Hive) and is renamed to `<out>` on success; on failure the tool exits non-zero with a clear
message and leaves no partial `<out>`.

## 6. Hive tables and Spark jobs

### 6.1 Tables (`hive/schema.sql`)

| Table | Format | Partition | Content |
|---|---|---|---|
| `events` | ORC | `dt` | compacted events, typed |
| `fs_inode`, `fs_edge`, `fs_block_inode`, `fs_meta` | ORC | `dt` (image date) | mapper output |
| `event_path` | ORC | `dt` | events + `inode_id`, `table_dir` (`path` is filled in) |
| `traffic_minute` | ORC | `dt` | `minute, user, op, role, locality, table_dir, bytes, est_bytes, ops`; `minute` = epoch ms of the UTC minute start |

All tables are EXTERNAL and ORC with ZLIB compression (Spark writes them with
`compression=zlib`). Database name and root directory are parameters. There is no Hive table over the raw
JSONL: the jobs read the files directly (a Hive JSON table would need the HCatalog
SerDe). The DDL source of truth is `org.hadooptraffic.spark.Ddl`; `hive/schema.sql` is
generated from it.

Each `fs_*` table is EXTERNAL ORC partitioned by `dt`, and every mapper run is registered
explicitly, one partition per table:
`ALTER TABLE fs_<t> ADD IF NOT EXISTS PARTITION (dt='X') LOCATION '<root>/fsimage/dt=X/<t>'`.
The mapper's `<out>` holds four tables with different schemas, so `MSCK REPAIR TABLE` must
never be used on these tables. Registration is done by `ResolvePaths` before it reads.

### 6.2 `CompactEvents --dt D`

Reads the files `raw/dt=D/dn=*/*.jsonl` directly, casts to the typed schema, writes `events/dt=D`
(overwrite of that partition only). Malformed lines are counted and skipped.
Only `*.jsonl` files are read: an in-flight `hdfs dfs -put` leaves
`._COPYING_` files in the partition. A missing partition (no matching file) fails the job.

### 6.3 `ResolvePaths --dt D [--image-dt I]`

1. Image: `I`, default the latest image — the latest `dt=` directory under
   `<root>/fsimage/` whose `meta/` contains an `*.orc` file (Spark-written test
   data and block-mapper output alike; hidden temp directories are ignored by
   name). This gives current-path semantics: files created during `D` are
   present in the image of `D+1`, meaning a NameNode checkpoint taken after
   midnight UTC (`fetchImage` returns the last checkpoint, not the live
   namespace). A WARN is logged when the image date is not after `D`.
2. Distinct `file_block_id` of `events/dt=D` → `fs_block_inode` → needed
   inode ids.
3. Ancestor closure: climb level by level from the needed inodes, joining the
   frontier with `fs_edge` (deduplicated, persisted and repartitioned by
   `child_id`, not broadcast), until only the root is left (one
   iteration per tree level, capped at 256 with a warning). The closure is
   small compared with the namespace.
4. Paths to the root by pointer jumping inside the closure: each iteration
   joins a node with its current ancestor, prepends that ancestor's name
   chain and doubles the step (≈ log₂(depth) iterations).
5. Names from `fs_inode` are concatenated root-first into `path`.
6. `event_path/dt=D`: every event with `inode_id` and `path`; WebHDFS HTTP-leg
   events take `path` from the event; unresolved blocks get `?`.
7. `traffic_minute/dt=D`: grouped by minute of `ts` and `table_dir`;
   `ops` = count of `last` events. Tables do not sit at a fixed depth
   (`/data/warehouse/db/t` and `/data/warehouse/x/db/t` both occur), so
   `table_dir` is derived from the file path by a heuristic, without the
   Hive metastore (user decision):
   - drop the file name;
   - cut the directory path at the first component that is a partition
     (`key=value`), a service directory (any name starting with `.` or `_`:
     `.hive-staging_*`, `.spark-staging-*`, `_temporary`, …) or a Hive ACID
     directory (`base_<n>`, `delta_<n>_<n>[_<n>]`, `delete_delta_<n>_<n>[_<n>]`, each with
     an optional `_v<n>` suffix);
     everything from that component on (including `-ext-10000`, attempt and
     bucket directories inside staging) is dropped;
   - what is left is `table_dir`; `?` stays `?`.
   Staging files: a file written into `<table>/.hive-staging_*/…` or
   `<table>/_temporary/…` keeps its inode and block ids when the job renames
   it into place, so with an image taken after the rename the path is already
   the final one; if the image was taken while the job still ran, the cut at
   the staging component still yields `<table>`. Files deleted before the
   next checkpoint (staging files of a failed job, Hive scratch directories,
   job staging jars, temporary outputs) are absent from the image and resolve
   to `?`. Renames move no
   data, so nothing is counted twice.
   Known limit: an unpartitioned table whose files sit in plain
   subdirectories folds one level lower (to the subdirectory).

Recommended daily order (`deploy/daily-report.sh`, cron at 02:00 UTC, after a
post-midnight checkpoint): the script first gates on the active NameNode's
`LastCheckpointTime` (JMX) being after 00:00 UTC of today, polling up to a
limit and failing loudly otherwise; it never calls `saveNamespace` in
production (it blocks the active NameNode in safe mode). It then fetches the
image, runs the mapper into `fsimage/dt=<today>` (skipped when that exists),
and runs `CompactEvents` and `ResolvePaths` for the previous day with
`--image-dt <today>`. `-D` reruns a given day against the latest image.

## 7. Fault tolerance

- The DataNode never blocks or fails because of the sink: bounded queue,
  drops with counters, retries on I/O errors.
- Lost delivery windows show up as `EventsDropped` / `EventFilesDropped`;
  totals can be cross-checked against JMX.
- Mapper and Spark jobs are re-runnable per partition.

## 8. Testing

- Agent unit: delta emission with an injected clock, `last` / `Ops` in the
  aggregator, `fileBlockId` normalization, JSON escaping, rotation by size and
  by age, file naming, `eventsMaxFiles` cleanup, drops on a full queue,
  option parsing.
- Agent IT (existing MiniDFSCluster tests, plus `events=`): per user, the sum
  of `bytes` in event files equals JMX `Bytes`, and the number of `last`
  events equals `Ops`; WebHDFS events carry `path`.
- Mapper IT: MiniDFSCluster with directories, files, a directory rename, a
  snapshot followed by a rename out of the snapshotted directory, and an EC
  file (`XOR-2-1`, 3 DataNodes); `saveNamespace`, run the mapper, check that
  for every block the path assembled from the output equals the path from the
  NameNode API. Both profiles, both formats. `FsImageReaderIT` and
  `FsImageMapperIT` share the `ImageFixture` (directory rename, snapshot + rename
  out, delete after a snapshot, `XOR-2-1-1024k` EC file).
- Spark jobs: JUnit with a local `SparkSession` on synthetic ORC inputs in the
  block-mapper schemas (the mapper → Spark file compatibility is checked on the
  stand), and an embedded Derby metastore for `HiveCatalogIT`; cases: pointer jumping, `?`, WebHDFS path,
  `table_dir` heuristic (partitions at varying depth, staging and
  `_temporary` directories, unpartitioned tables), `ops` counting.
- Stand (`stand/run-scenario.sh`, warehouse step, runs on `tr-nn`): alice
  writes into `.hive-staging` and moves the file out to the table, bob writes
  one level deeper (`/warehouse/extra/db2/t2`), carol's file stays in staging;
  events are shipped, then stand-only `safemode enter` / `saveNamespace` /
  `safemode leave` forces a checkpoint before `fetchImage` (`fetchImage`
  returns the last checkpoint; this is forbidden in production), the mapper
  and `spark-submit --master local` (`--catalog none`) run, and
  `check_traffic.py tables` checks the bytes by `table_dir` on `event_path`.
  The YARN / Kerberos / Hive metastore path is not covered by the stand.

## Out of scope

- Edit log tailing for fresher mappings (stage 3).
- Kafka, ClickHouse, real-time dashboards.
- Path as of the time of the operation.
- NameNode-side agent.
- Workflow orchestration (Oozie / Airflow); only cron examples are provided.
