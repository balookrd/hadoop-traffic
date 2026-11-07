# hdfs-traffic-agent: data example from events to report

[Русская версия](data-examples.md) · [Overview](../README.en.md)

This document follows one scenario through every step and shows the rows each step produces:
the agent's JSONL, `events`, the block-mapper tables, `event_path` and `traffic_minute`. It is
for people who write queries against the reports and want to know where every number comes
from. The columns are described in the [README](../README.en.md#reports-spark-jobs).

The data is made up, not taken from a stand run, but it follows the code of `EventJson`,
`CompactEvents`, `ResolvePaths`, `PathResolver` and `TableDir`. Values agree across tables:
the sums in `traffic_minute` are computed from the `event_path` rows below.

Notation:

| Notation | Meaning |
|---|---|
| `dn1`, `dn2`, `dn3` | DataNodes `dn1.example.com`, `dn2.example.com`, `dn3.example.com` |
| `…/orders` | `/warehouse/tablespace/managed/hive/sales.db/orders` |
| time `10:15:50.000` | UTC, 2026-11-06; tables store epoch ms (`1793960150000`) |
| `<root>` | root of the report data, `/data/hdfs-traffic` by default |

## Scenario

Day `2026-11-06`, `flushIntervalSec=30`.

| # | What happens | Expected result |
|---|---|---|
| A | `alice` (a Hive job on host dn1) writes block `1073741901` of 128 MiB to `…/orders/dt=2026-11-05/delta_0000012_0000012/bucket_00000`. Write pipeline: dn1 → dn2 → dn3. The operation takes 52 s | two events on each DataNode, `table_dir = …/orders` |
| B | `bob` on host dn2 reads that file via short-circuit | one `scr_fds` event with `bytes = 0` |
| C | `etl` runs `hdfs dfs -put` over WebHDFS to `/landing/crm/clients.csv` (10 MiB, block `1073741955`) | two events on dn1: the HTTP leg and the internal `webhdfs_proxy` leg |
| D | `alice` writes 2 MiB (block `1073742010`) to the scratch directory `/tmp/hive/alice/…`, deleted before the NameNode checkpoint | the path is not found, `path = ?` |

Replica events for C and D are left out to keep the tables short.

## Step 1. Agent JSONL

The agent on every DataNode writes a local file; `deploy/upload-events.sh` puts it into
`<root>/raw/dt=2026-11-06/dn=<host>/`. Lines of operation A on dn1:

```json
{"ts":1793960150000,"start":1793960120000,"dn":"dn1.example.com","op_id":"a1f3c2-17","user":"alice","op":"write","role":"client","locality":"local","bpid":"BP-1","block_id":1073741901,"file_block_id":1073741901,"bytes":83886080,"est_bytes":0,"dur_ms":30000,"last":false,"peer":"/10.0.0.11:51234","path":null}
{"ts":1793960172000,"start":1793960120000,"dn":"dn1.example.com","op_id":"a1f3c2-17","user":"alice","op":"write","role":"client","locality":"local","bpid":"BP-1","block_id":1073741901,"file_block_id":1073741901,"bytes":50331648,"est_bytes":0,"dur_ms":52000,"last":true,"peer":"/10.0.0.11:51234","path":null}
```

What these two lines show:

- They share `start` and `op_id`: it is one operation.
- `bytes` is the increment since the previous event: 80 MiB by 10:15:50 and 48 MiB more by
  10:16:12, 128 MiB in total.
- `dur_ms` is the time since the operation started, not an increment: 30000, then 52000. The
  operation's duration is the `dur_ms` of its last event.
- The first event is emitted 30 s after the start because `flushIntervalSec=30`. Without it
  the whole block would arrive as one event at 10:16:12.

## Step 2. `events` (CompactEvents)

`CompactEvents --dt 2026-11-06` reads every `raw/dt=2026-11-06/dn=*/*.jsonl` and writes it
to typed ORC without changing values. The columns `bpid`, `file_block_id` and `peer` are not
shown: `bpid` is `BP-1` everywhere, `file_block_id` equals `block_id` (no erasure-coded
blocks).

| ts | start | dur_ms | dn | op_id | user | op | role | locality | block_id | bytes | est_bytes | last | path |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 10:15:39.950 | 10:15:36.200 | 3750 | dn1 | a1f3c2-19 | etl | write | webhdfs_proxy | local | 1073741955 | 10485760 | 0 | true | NULL |
| 10:15:40.000 | 10:15:36.000 | 4000 | dn1 | a1f3c2-18 | etl | webhdfs_create | client | remote | 1073741955 | 10485760 | 0 | true | `/landing/crm/clients.csv._COPYING_` |
| 10:15:50.000 | 10:15:20.000 | 30000 | dn1 | a1f3c2-17 | alice | write | client | local | 1073741901 | 83886080 | 0 | false | NULL |
| 10:15:50.000 | 10:15:20.010 | 29990 | dn2 | 7be019-40 | alice | write | pipeline | remote | 1073741901 | 83886080 | 0 | false | NULL |
| 10:15:50.000 | 10:15:20.020 | 29980 | dn3 | c04d88-5 | alice | write | pipeline | remote | 1073741901 | 83886080 | 0 | false | NULL |
| 10:15:55.000 | 10:15:54.000 | 1000 | dn3 | c04d88-6 | alice | write | client | remote | 1073742010 | 2097152 | 0 | true | NULL |
| 10:16:12.000 | 10:15:20.000 | 52000 | dn1 | a1f3c2-17 | alice | write | client | local | 1073741901 | 50331648 | 0 | true | NULL |
| 10:16:12.000 | 10:15:20.010 | 51990 | dn2 | 7be019-40 | alice | write | pipeline | remote | 1073741901 | 50331648 | 0 | true | NULL |
| 10:16:12.000 | 10:15:20.020 | 51980 | dn3 | c04d88-5 | alice | write | pipeline | remote | 1073741901 | 50331648 | 0 | true | NULL |
| 10:16:30.002 | 10:16:30.000 | 2 | dn2 | 7be019-41 | bob | scr_fds | client | local | 1073741901 | 0 | 134217728 | true | NULL |

Notes:

- dn1 sees `alice` as `client`/`local`: the client runs on the same host and connected over
  the same IP. dn2 and dn3 receive data from the previous DataNode of the pipeline, so they
  record `pipeline`/`remote`. `bob` connects over a UNIX domain socket, which is always
  `local`.
- `op_id` is unique only within a DataNode (its prefix is a random per-process number), so an
  operation in the cluster is identified by the pair (`dn`, `op_id`).
- WebHDFS produces two records of one transfer: `webhdfs_create` is the HTTP leg from the
  client to dn1, `write`/`webhdfs_proxy` is the block write by the DFSClient embedded in dn1.
  Only the HTTP leg has a path, and it is the temporary name `._COPYING_`.
- `scr_fds` hands out file descriptors for a short-circuit read. The client reads the local
  disk itself, so `bytes = 0`, `est_bytes` holds the block length, and `dur_ms = 2` is the time
  to hand out the descriptors, not the read time.

## Step 3. fsimage model (block-mapper)

After midnight `daily-report.sh` fetches the NameNode checkpoint and runs block-mapper; the
result is in `<root>/fsimage/dt=2026-11-07/`. The image is from the next day: files created on
2026-11-06 appear only in a checkpoint taken after that day ends. Rows needed for the
scenario:

`fs_inode` (`type`: `F` file, `D` directory, `S` symlink; the root's name is empty):

| inode_id | type | name |
|---|---|---|
| 16385 | D | `` |
| 16386 | D | `warehouse` |
| 16390 | D | `tablespace` |
| 16391 | D | `managed` |
| 16392 | D | `hive` |
| 16400 | D | `sales.db` |
| 16410 | D | `orders` |
| 16415 | D | `dt=2026-11-05` |
| 16418 | D | `delta_0000012_0000012` |
| 16420 | F | `bucket_00000` |
| 16500 | D | `landing` |
| 16505 | D | `crm` |
| 16512 | F | `clients.csv` |

`fs_edge` (current parent):

| child_id | parent_id |
|---|---|
| 16386 | 16385 |
| 16390 | 16386 |
| 16391 | 16390 |
| 16392 | 16391 |
| 16400 | 16392 |
| 16410 | 16400 |
| 16415 | 16410 |
| 16418 | 16415 |
| 16420 | 16418 |
| 16500 | 16385 |
| 16505 | 16500 |
| 16512 | 16505 |

`fs_block_inode`:

| block_id | inode_id |
|---|---|
| 1073741901 | 16420 |
| 1073741955 | 16512 |

Block `1073742010` is not in the image: the file under `/tmp/hive/alice/…` was deleted before
the checkpoint.

`fs_meta`:

| txid | image_file | parsed_at |
|---|---|---|
| 4815162 | `fsimage_0000000000004815162` | 1794018720000 |

## Step 4. `event_path` (ResolvePaths)

`ResolvePaths --dt 2026-11-06 --image-dt 2026-11-07` does the following:

1. Takes the `file_block_id` of the day's events and looks up their `inode_id` in
   `fs_block_inode`.
2. Climbs `fs_edge` to the root (16385) and joins the names from `fs_inode` into a path.
3. Fills `path`: the event's path if it has one (WebHDFS), otherwise the path from the image,
   otherwise `?`.
4. Computes `table_dir` from `path`: drops the file name and cuts at the first component of the
   form `key=value`, `.x`, `_x`, `base_N`, `delta_N_N` or `delete_delta_N_N`.

Event columns are carried over unchanged, `start` and `dur_ms` included. Only the columns that
help follow the result are shown:

| ts | dn | user | op | role | dur_ms | bytes | path | inode_id | table_dir |
|---|---|---|---|---|---|---|---|---|---|
| 10:15:39.950 | dn1 | etl | write | webhdfs_proxy | 3750 | 10485760 | `/landing/crm/clients.csv` | 16512 | `/landing/crm` |
| 10:15:40.000 | dn1 | etl | webhdfs_create | client | 4000 | 10485760 | `/landing/crm/clients.csv._COPYING_` | 16512 | `/landing/crm` |
| 10:15:50.000 | dn1 | alice | write | client | 30000 | 83886080 | `…/orders/dt=2026-11-05/delta_0000012_0000012/bucket_00000` | 16420 | `…/orders` |
| 10:15:50.000 | dn2 | alice | write | pipeline | 29990 | 83886080 | same | 16420 | `…/orders` |
| 10:15:50.000 | dn3 | alice | write | pipeline | 29980 | 83886080 | same | 16420 | `…/orders` |
| 10:15:55.000 | dn3 | alice | write | client | 1000 | 2097152 | `?` | NULL | `?` |
| 10:16:12.000 | dn1 | alice | write | client | 52000 | 50331648 | same | 16420 | `…/orders` |
| 10:16:12.000 | dn2 | alice | write | pipeline | 51990 | 50331648 | same | 16420 | `…/orders` |
| 10:16:12.000 | dn3 | alice | write | pipeline | 51980 | 50331648 | same | 16420 | `…/orders` |
| 10:16:30.002 | dn2 | bob | scr_fds | client | 2 | 0 | same | 16420 | `…/orders` |

"same" is the path `…/orders/dt=2026-11-05/delta_0000012_0000012/bucket_00000`.

Notes:

- The WebHDFS HTTP leg keeps the event's path (`._COPYING_`) even though `inode_id` was found
  by block. The internal leg gets the final name from the image. Both get the same
  `table_dir` because the file name is dropped.
- For `orders` the cut happens at `dt=2026-11-05`. For an unpartitioned table it would happen
  at `delta_0000012_0000012`, with the same `table_dir`.
- Row D got `?`: the block is not in `fs_block_inode` and the event has no `path`. The final
  `ResolvePaths` line for this day: `events=10 unresolved=1`.

## Step 5. `traffic_minute` (ResolvePaths)

`event_path` is grouped by `minute, user, op, role, locality, table_dir`. `minute` is the start
of the `ts` minute in epoch ms; `bytes` and `est_bytes` are summed; `ops` counts events with
`last = true`.

| minute | user | op | role | locality | table_dir | bytes | est_bytes | ops |
|---|---|---|---|---|---|---|---|---|
| 1793960100000 (10:15) | etl | write | webhdfs_proxy | local | `/landing/crm` | 10485760 | 0 | 1 |
| 1793960100000 (10:15) | etl | webhdfs_create | client | remote | `/landing/crm` | 10485760 | 0 | 1 |
| 1793960100000 (10:15) | alice | write | client | local | `…/orders` | 83886080 | 0 | 0 |
| 1793960100000 (10:15) | alice | write | pipeline | remote | `…/orders` | 167772160 | 0 | 0 |
| 1793960100000 (10:15) | alice | write | client | remote | `?` | 2097152 | 0 | 1 |
| 1793960160000 (10:16) | alice | write | client | local | `…/orders` | 50331648 | 0 | 1 |
| 1793960160000 (10:16) | alice | write | pipeline | remote | `…/orders` | 100663296 | 0 | 2 |
| 1793960160000 (10:16) | bob | scr_fds | client | local | `…/orders` | 0 | 134217728 | 1 |

Notes:

- The bytes of operation A are split between two minutes by event emission time: 80 MiB at
  10:15, 48 MiB at 10:16. The operation is counted in `ops` once, in the minute of its last
  event.
- `pipeline` at 10:15 is 2 × 80 MiB: the dn2 and dn3 events fall into one group.
- `start` and `dur_ms` are not carried into `traffic_minute`. The speed of a single operation
  can only be computed from `event_path` (see below).

## Queries over the result

Write throughput per table per minute, MiB/s. Operation durations are not needed here: the
minute's bytes are divided by 60 s. `role = 'client'` keeps one copy of the data, without
pipeline replicas and without the internal WebHDFS leg:

```sql
SELECT minute, table_dir, SUM(bytes) / 1048576 / 60 AS mib_per_s
FROM hdfs_traffic.traffic_minute
WHERE dt = '2026-11-06'
  AND op IN ('write', 'webhdfs_create', 'webhdfs_append') AND role = 'client'
GROUP BY minute, table_dir
ORDER BY minute, mib_per_s DESC;
```

Result on the example data:

| minute | table_dir | mib_per_s |
|---|---|---|
| 10:15 | `…/orders` | 1.333 |
| 10:15 | `/landing/crm` | 0.167 |
| 10:15 | `?` | 0.033 |
| 10:16 | `…/orders` | 0.8 |

Speed of every finished operation, MiB/s. The duration is `MAX(dur_ms)` because `dur_ms` grows
within an operation:

```sql
SELECT dn, op_id, `user`, op, role, table_dir,
       SUM(bytes)                                    AS bytes,
       MAX(dur_ms)                                   AS dur_ms,
       SUM(bytes) / 1048576 / (MAX(dur_ms) / 1000)   AS mib_per_s
FROM hdfs_traffic.event_path
WHERE dt = '2026-11-06'
GROUP BY dn, op_id, `user`, op, role, table_dir
HAVING MAX(CAST(`last` AS INT)) = 1 AND MAX(dur_ms) > 0;
```

Result on the example data:

| dn | op_id | user | op | role | table_dir | bytes | dur_ms | mib_per_s |
|---|---|---|---|---|---|---|---|---|
| dn1 | a1f3c2-17 | alice | write | client | `…/orders` | 134217728 | 52000 | 2.462 |
| dn2 | 7be019-40 | alice | write | pipeline | `…/orders` | 134217728 | 51990 | 2.462 |
| dn3 | c04d88-5 | alice | write | pipeline | `…/orders` | 134217728 | 51980 | 2.462 |
| dn1 | a1f3c2-18 | etl | webhdfs_create | client | `/landing/crm` | 10485760 | 4000 | 2.5 |
| dn1 | a1f3c2-19 | etl | write | webhdfs_proxy | `/landing/crm` | 10485760 | 3750 | 2.667 |
| dn3 | c04d88-6 | alice | write | client | `?` | 2097152 | 1000 | 2.0 |
| dn2 | 7be019-41 | bob | scr_fds | client | `…/orders` | 0 | 2 | 0.0 |

## Known limitations of these calculations

- `dur_ms` is wall-clock time since the operation started on the DataNode. It includes idle
  time: a slow client, waiting for the ack of the next pipeline DataNode, pauses between reads.
  `bytes / dur_ms` is the effective speed of the operation, not the network speed.
- No speed for `scr_fds`: `bytes = 0`, and `dur_ms` is the time to hand out descriptors.
- An operation running across UTC midnight is split between two `dt` partitions: a closed
  event file carries the date of its events, and `upload-events.sh` puts it into that date's
  partition. The speed query above keeps the part with `last = true`, whose `MAX(dur_ms)` is
  the full operation time, so the speed comes out too low.
- The sum of `bytes` over all roles is the DataNodes' physical traffic, not the data volume: a
  write with replication 3 is counted three times, a WebHDFS `put` twice on the first
  DataNode.
