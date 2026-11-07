# hdfs-traffic-agent: data example from events to report

[Русская версия](data-examples.md) · [Overview](../README.en.md)

This document follows one scenario through every step and shows the rows each step produces:
the agent's JSONL, `events`, the block-mapper tables, `event_path` and `traffic_minute`. It is
for people who write queries against the reports and want to know where every number comes
from. The columns are described in the [README](../README.en.md#reports-spark-jobs).

The data is made up, not taken from a stand run, but it follows the code of `EventJson`,
`WebHdfsHooks`, `XceiverHooks`, `CompactEvents`, `ResolvePaths`, `PathResolver` and
`TableDir`. Every step shows all 22 events of the scenario. The `#` column is a running event
number: it finds an `events` row in `event_path`, and the "rows" column of `traffic_minute`
tells which events a group is built from.

Notation:

| Notation | Meaning |
|---|---|
| `dn1`, `dn2`, `dn3` | DataNodes `dn1.example.com` (10.0.0.11), `dn2.example.com` (10.0.0.12), `dn3.example.com` (10.0.0.13) |
| time `10:15:50.000` | UTC, 2026-11-06; tables store epoch ms (`1793960150000`) |
| `BP-1` | block pool id, shortened; a real cluster has the form `BP-<n>-<ip>-<ms>` |
| `<root>` | root of the report data, `/data/hdfs-traffic` by default |

## Scenario

Day `2026-11-06`, `flushIntervalSec=30`, replication 3. There are no other operations in the
cluster.

| Letter | What happens | Events |
|---|---|---|
| A | `alice` (a Hive task on host dn1) writes block `1073741901` of 128 MiB to `/warehouse/tablespace/managed/hive/sales.db/orders/dt=2026-11-05/delta_0000012_0000012/bucket_00000`. Write pipeline: dn1 → dn2 → dn3. The operation takes about 52 s | 6: two on each DataNode (an intermediate one after 30 s and the last one) |
| B | `bob` on host dn2 reads that file via short-circuit | 1: `scr_fds` on dn2 |
| E | `carol` (a Spark task on host 10.0.0.22, which runs no DataNode) reads the same block in full over the network from dn3. The read takes 40.5 s | 2 on dn3: an intermediate one after 30 s and the last one |
| C | `etl` on host 10.0.0.50 runs `hdfs dfs -put` over WebHDFS to `/landing/crm/clients.csv` (10 MiB, block `1073741955`). DataNode dn1 accepts the HTTP request and writes the block with its embedded DFSClient through the pipeline dn1 → dn2 → dn3 | 4: the HTTP leg and `webhdfs_proxy` on dn1, replicas on dn2 and dn3 |
| D | `alice` (a Hive task on host 10.0.0.21, which runs no DataNode) writes 2 MiB (block `1073742010`) to the scratch directory `/tmp/hive/alice/…`, deleted before the NameNode checkpoint. Pipeline: dn3 → dn1 → dn2 | 3: one on each DataNode |
| F | `radcom` (a Hive task on host dn2) writes 64 MiB (block `1073742100`) to the external table `/warehouse/tablespace/external/hive/radcom.db/cdr/gtpu`, partition `dt=2026-11-06/hour=10`, file `000000_0`. Pipeline: dn2 → dn3 → dn1, 12 s | 3: one on each DataNode |
| G | `radcom` (a Hive task on host 10.0.0.23, which runs no DataNode) writes 16 MiB (block `1073742120`) to the external table `/warehouse/tablespace/external/hive/radcom.db/cdr/s1ap`, partition `dt=2026-11-06/hour=10`, file `000000_0`. Pipeline: dn1 → dn3 → dn2, 4 s | 3: one on each DataNode |

## Step 1. Agent JSONL

The agent on every DataNode writes events to a local file and rotates it to
`events-<yyyyMMdd-HHmmss>-<seq>.jsonl`, where the time is the UTC time of the file's first
event. `deploy/upload-events.sh` puts closed files into
`<root>/raw/dt=2026-11-06/dn=<host>/`. Lines in a file follow the order of emission.

`<root>/raw/dt=2026-11-06/dn=dn1.example.com/events-20261106-101539-000007.jsonl`:

```json
{"ts":1793960139950,"start":1793960136200,"dn":"dn1.example.com","op_id":"a1f3c2-19","user":"etl","op":"write","role":"webhdfs_proxy","locality":"local","bpid":"BP-1","block_id":1073741955,"file_block_id":1073741955,"bytes":10485760,"est_bytes":0,"dur_ms":3750,"last":true,"peer":"/10.0.0.11:51310","path":null}
{"ts":1793960140000,"start":1793960136000,"dn":"dn1.example.com","op_id":"a1f3c2-18","user":"etl","op":"webhdfs_create","role":"client","locality":"remote","bpid":null,"block_id":-1,"file_block_id":-1,"bytes":10485760,"est_bytes":0,"dur_ms":4000,"last":true,"peer":"/10.0.0.50:41234","path":"/landing/crm/clients.csv._COPYING_"}
{"ts":1793960150000,"start":1793960120000,"dn":"dn1.example.com","op_id":"a1f3c2-17","user":"alice","op":"write","role":"client","locality":"local","bpid":"BP-1","block_id":1073741901,"file_block_id":1073741901,"bytes":83886080,"est_bytes":0,"dur_ms":30000,"last":false,"peer":"/10.0.0.11:51234","path":null}
{"ts":1793960154990,"start":1793960154010,"dn":"dn1.example.com","op_id":"a1f3c2-20","user":"alice","op":"write","role":"pipeline","locality":"remote","bpid":"BP-1","block_id":1073742010,"file_block_id":1073742010,"bytes":2097152,"est_bytes":0,"dur_ms":980,"last":true,"peer":"/10.0.0.13:52004","path":null}
{"ts":1793960172000,"start":1793960120000,"dn":"dn1.example.com","op_id":"a1f3c2-17","user":"alice","op":"write","role":"client","locality":"local","bpid":"BP-1","block_id":1073741901,"file_block_id":1073741901,"bytes":50331648,"est_bytes":0,"dur_ms":52000,"last":true,"peer":"/10.0.0.11:51234","path":null}
{"ts":1793960216980,"start":1793960205020,"dn":"dn1.example.com","op_id":"a1f3c2-21","user":"radcom","op":"write","role":"pipeline","locality":"remote","bpid":"BP-1","block_id":1073742100,"file_block_id":1073742100,"bytes":67108864,"est_bytes":0,"dur_ms":11960,"last":true,"peer":"/10.0.0.13:52080","path":null}
{"ts":1793960229000,"start":1793960225000,"dn":"dn1.example.com","op_id":"a1f3c2-22","user":"radcom","op":"write","role":"client","locality":"remote","bpid":"BP-1","block_id":1073742120,"file_block_id":1073742120,"bytes":16777216,"est_bytes":0,"dur_ms":4000,"last":true,"peer":"/10.0.0.23:51522","path":null}
```

`<root>/raw/dt=2026-11-06/dn=dn2.example.com/events-20261106-101539-000007.jsonl`:

```json
{"ts":1793960139940,"start":1793960136210,"dn":"dn2.example.com","op_id":"7be019-41","user":"etl","op":"write","role":"pipeline","locality":"remote","bpid":"BP-1","block_id":1073741955,"file_block_id":1073741955,"bytes":10485760,"est_bytes":0,"dur_ms":3730,"last":true,"peer":"/10.0.0.11:47790","path":null}
{"ts":1793960150010,"start":1793960120010,"dn":"dn2.example.com","op_id":"7be019-40","user":"alice","op":"write","role":"pipeline","locality":"remote","bpid":"BP-1","block_id":1073741901,"file_block_id":1073741901,"bytes":83886080,"est_bytes":0,"dur_ms":30000,"last":false,"peer":"/10.0.0.11:47710","path":null}
{"ts":1793960154980,"start":1793960154020,"dn":"dn2.example.com","op_id":"7be019-42","user":"alice","op":"write","role":"pipeline","locality":"remote","bpid":"BP-1","block_id":1073742010,"file_block_id":1073742010,"bytes":2097152,"est_bytes":0,"dur_ms":960,"last":true,"peer":"/10.0.0.11:47850","path":null}
{"ts":1793960171990,"start":1793960120010,"dn":"dn2.example.com","op_id":"7be019-40","user":"alice","op":"write","role":"pipeline","locality":"remote","bpid":"BP-1","block_id":1073741901,"file_block_id":1073741901,"bytes":50331648,"est_bytes":0,"dur_ms":51980,"last":true,"peer":"/10.0.0.11:47710","path":null}
{"ts":1793960190002,"start":1793960190000,"dn":"dn2.example.com","op_id":"7be019-43","user":"bob","op":"scr_fds","role":"client","locality":"local","bpid":"BP-1","block_id":1073741901,"file_block_id":1073741901,"bytes":0,"est_bytes":134217728,"dur_ms":2,"last":true,"peer":"unix:/var/lib/hadoop-hdfs/dn_socket","path":null}
{"ts":1793960217000,"start":1793960205000,"dn":"dn2.example.com","op_id":"7be019-44","user":"radcom","op":"write","role":"client","locality":"local","bpid":"BP-1","block_id":1073742100,"file_block_id":1073742100,"bytes":67108864,"est_bytes":0,"dur_ms":12000,"last":true,"peer":"/10.0.0.12:52310","path":null}
{"ts":1793960228980,"start":1793960225020,"dn":"dn2.example.com","op_id":"7be019-45","user":"radcom","op":"write","role":"pipeline","locality":"remote","bpid":"BP-1","block_id":1073742120,"file_block_id":1073742120,"bytes":16777216,"est_bytes":0,"dur_ms":3960,"last":true,"peer":"/10.0.0.13:38990","path":null}
```

`<root>/raw/dt=2026-11-06/dn=dn3.example.com/events-20261106-101539-000007.jsonl`:

```json
{"ts":1793960139930,"start":1793960136220,"dn":"dn3.example.com","op_id":"c04d88-6","user":"etl","op":"write","role":"pipeline","locality":"remote","bpid":"BP-1","block_id":1073741955,"file_block_id":1073741955,"bytes":10485760,"est_bytes":0,"dur_ms":3710,"last":true,"peer":"/10.0.0.12:38900","path":null}
{"ts":1793960150020,"start":1793960120020,"dn":"dn3.example.com","op_id":"c04d88-5","user":"alice","op":"write","role":"pipeline","locality":"remote","bpid":"BP-1","block_id":1073741901,"file_block_id":1073741901,"bytes":83886080,"est_bytes":0,"dur_ms":30000,"last":false,"peer":"/10.0.0.12:38822","path":null}
{"ts":1793960155000,"start":1793960154000,"dn":"dn3.example.com","op_id":"c04d88-7","user":"alice","op":"write","role":"client","locality":"remote","bpid":"BP-1","block_id":1073742010,"file_block_id":1073742010,"bytes":2097152,"est_bytes":0,"dur_ms":1000,"last":true,"peer":"/10.0.0.21:60112","path":null}
{"ts":1793960171980,"start":1793960120020,"dn":"dn3.example.com","op_id":"c04d88-5","user":"alice","op":"write","role":"pipeline","locality":"remote","bpid":"BP-1","block_id":1073741901,"file_block_id":1073741901,"bytes":50331648,"est_bytes":0,"dur_ms":51960,"last":true,"peer":"/10.0.0.12:38822","path":null}
{"ts":1793960216990,"start":1793960205010,"dn":"dn3.example.com","op_id":"c04d88-9","user":"radcom","op":"write","role":"pipeline","locality":"remote","bpid":"BP-1","block_id":1073742100,"file_block_id":1073742100,"bytes":67108864,"est_bytes":0,"dur_ms":11980,"last":true,"peer":"/10.0.0.12:38950","path":null}
{"ts":1793960228990,"start":1793960225010,"dn":"dn3.example.com","op_id":"c04d88-10","user":"radcom","op":"write","role":"pipeline","locality":"remote","bpid":"BP-1","block_id":1073742120,"file_block_id":1073742120,"bytes":16777216,"est_bytes":0,"dur_ms":3980,"last":true,"peer":"/10.0.0.11:52120","path":null}
{"ts":1793960230000,"start":1793960200000,"dn":"dn3.example.com","op_id":"c04d88-8","user":"carol","op":"read","role":"client","locality":"remote","bpid":"BP-1","block_id":1073741901,"file_block_id":1073741901,"bytes":100663296,"est_bytes":0,"dur_ms":30000,"last":false,"peer":"/10.0.0.22:43120","path":null}
{"ts":1793960240500,"start":1793960200000,"dn":"dn3.example.com","op_id":"c04d88-8","user":"carol","op":"read","role":"client","locality":"remote","bpid":"BP-1","block_id":1073741901,"file_block_id":1073741901,"bytes":33554432,"est_bytes":0,"dur_ms":40500,"last":true,"peer":"/10.0.0.22:43120","path":null}
```

How to read the lines:

- `op_id` is `<6 hex>-<n>`: the prefix is random per DataNode process (`a1f3c2` on dn1,
  `7be019` on dn2, `c04d88` on dn3), `n` is the operation counter. An operation in the cluster
  is identified by the pair (`dn`, `op_id`).
- Operation A has two events on each DataNode with shared `start` and `op_id`. `bytes` is the
  increment since the previous event: 80 MiB, then 48 MiB, 128 MiB in total. `dur_ms` is the
  time since the operation started, not an increment: 30000, then ~52000.
- The intermediate event is emitted 30 s after the start because `flushIntervalSec=30`.
  Operations B, C, D, F and G are shorter than 30 s, so each has one event with `last: true`.
- The plain read of `carol` (E) works like write A: the DataNode sends the bytes to the client
  itself and counts them (payload, without checksums). 96 MiB are sent after 30 s, 32 MiB
  more by the close. Unlike `scr_fds`, `bytes` is exact here and `est_bytes = 0`.
- `peer` is the address of the other side of the connection. For replicas it is the previous
  DataNode of the pipeline (dn2 sees 10.0.0.11, dn3 sees 10.0.0.12); for `scr_fds` it is the
  UNIX domain socket path.
- The WebHDFS HTTP leg (`webhdfs_create`) does not know the block: `bpid: null`,
  `block_id: -1`. It has a `path` instead, taken from the HTTP request; here it is the
  temporary name `._COPYING_`. The block is written by the DFSClient embedded in dn1; that
  write shows up as `write`/`webhdfs_proxy`.
- `locality`: `alice` on dn1 connected from the same IP the connection arrived at, so it is
  `local`. The HTTP client `etl` on 10.0.0.50 is `remote`. `bob` connected over a domain
  socket, so it is `local`. `radcom` in F writes from dn2 to dn2, so it is `local`; in G it
  writes from 10.0.0.23 to dn1, so it is `remote`.

## Step 2. `events` (CompactEvents)

`CompactEvents --dt 2026-11-06` reads every `raw/dt=2026-11-06/dn=*/*.jsonl` and writes it to
typed ORC `<root>/events/dt=2026-11-06` without changing values. Rows here are sorted by `ts`
for readability; the job does not define an order.

| # | ts | start | dur_ms | dn | op_id | user | op | role | locality | bpid | block_id | file_block_id | bytes | est_bytes | last | peer | path |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 C | 10:15:39.930 | 10:15:36.220 | 3710 | dn3 | c04d88-6 | etl | write | pipeline | remote | `BP-1` | 1073741955 | 1073741955 | 10485760 | 0 | true | `/10.0.0.12:38900` | NULL |
| 2 C | 10:15:39.940 | 10:15:36.210 | 3730 | dn2 | 7be019-41 | etl | write | pipeline | remote | `BP-1` | 1073741955 | 1073741955 | 10485760 | 0 | true | `/10.0.0.11:47790` | NULL |
| 3 C | 10:15:39.950 | 10:15:36.200 | 3750 | dn1 | a1f3c2-19 | etl | write | webhdfs_proxy | local | `BP-1` | 1073741955 | 1073741955 | 10485760 | 0 | true | `/10.0.0.11:51310` | NULL |
| 4 C | 10:15:40.000 | 10:15:36.000 | 4000 | dn1 | a1f3c2-18 | etl | webhdfs_create | client | remote | NULL | -1 | -1 | 10485760 | 0 | true | `/10.0.0.50:41234` | `/landing/crm/clients.csv._COPYING_` |
| 5 A | 10:15:50.000 | 10:15:20.000 | 30000 | dn1 | a1f3c2-17 | alice | write | client | local | `BP-1` | 1073741901 | 1073741901 | 83886080 | 0 | false | `/10.0.0.11:51234` | NULL |
| 6 A | 10:15:50.010 | 10:15:20.010 | 30000 | dn2 | 7be019-40 | alice | write | pipeline | remote | `BP-1` | 1073741901 | 1073741901 | 83886080 | 0 | false | `/10.0.0.11:47710` | NULL |
| 7 A | 10:15:50.020 | 10:15:20.020 | 30000 | dn3 | c04d88-5 | alice | write | pipeline | remote | `BP-1` | 1073741901 | 1073741901 | 83886080 | 0 | false | `/10.0.0.12:38822` | NULL |
| 8 D | 10:15:54.980 | 10:15:54.020 | 960 | dn2 | 7be019-42 | alice | write | pipeline | remote | `BP-1` | 1073742010 | 1073742010 | 2097152 | 0 | true | `/10.0.0.11:47850` | NULL |
| 9 D | 10:15:54.990 | 10:15:54.010 | 980 | dn1 | a1f3c2-20 | alice | write | pipeline | remote | `BP-1` | 1073742010 | 1073742010 | 2097152 | 0 | true | `/10.0.0.13:52004` | NULL |
| 10 D | 10:15:55.000 | 10:15:54.000 | 1000 | dn3 | c04d88-7 | alice | write | client | remote | `BP-1` | 1073742010 | 1073742010 | 2097152 | 0 | true | `/10.0.0.21:60112` | NULL |
| 11 A | 10:16:11.980 | 10:15:20.020 | 51960 | dn3 | c04d88-5 | alice | write | pipeline | remote | `BP-1` | 1073741901 | 1073741901 | 50331648 | 0 | true | `/10.0.0.12:38822` | NULL |
| 12 A | 10:16:11.990 | 10:15:20.010 | 51980 | dn2 | 7be019-40 | alice | write | pipeline | remote | `BP-1` | 1073741901 | 1073741901 | 50331648 | 0 | true | `/10.0.0.11:47710` | NULL |
| 13 A | 10:16:12.000 | 10:15:20.000 | 52000 | dn1 | a1f3c2-17 | alice | write | client | local | `BP-1` | 1073741901 | 1073741901 | 50331648 | 0 | true | `/10.0.0.11:51234` | NULL |
| 14 B | 10:16:30.002 | 10:16:30.000 | 2 | dn2 | 7be019-43 | bob | scr_fds | client | local | `BP-1` | 1073741901 | 1073741901 | 0 | 134217728 | true | `unix:/var/lib/hadoop-hdfs/dn_socket` | NULL |
| 15 F | 10:16:56.980 | 10:16:45.020 | 11960 | dn1 | a1f3c2-21 | radcom | write | pipeline | remote | `BP-1` | 1073742100 | 1073742100 | 67108864 | 0 | true | `/10.0.0.13:52080` | NULL |
| 16 F | 10:16:56.990 | 10:16:45.010 | 11980 | dn3 | c04d88-9 | radcom | write | pipeline | remote | `BP-1` | 1073742100 | 1073742100 | 67108864 | 0 | true | `/10.0.0.12:38950` | NULL |
| 17 F | 10:16:57.000 | 10:16:45.000 | 12000 | dn2 | 7be019-44 | radcom | write | client | local | `BP-1` | 1073742100 | 1073742100 | 67108864 | 0 | true | `/10.0.0.12:52310` | NULL |
| 18 G | 10:17:08.980 | 10:17:05.020 | 3960 | dn2 | 7be019-45 | radcom | write | pipeline | remote | `BP-1` | 1073742120 | 1073742120 | 16777216 | 0 | true | `/10.0.0.13:38990` | NULL |
| 19 G | 10:17:08.990 | 10:17:05.010 | 3980 | dn3 | c04d88-10 | radcom | write | pipeline | remote | `BP-1` | 1073742120 | 1073742120 | 16777216 | 0 | true | `/10.0.0.11:52120` | NULL |
| 20 G | 10:17:09.000 | 10:17:05.000 | 4000 | dn1 | a1f3c2-22 | radcom | write | client | remote | `BP-1` | 1073742120 | 1073742120 | 16777216 | 0 | true | `/10.0.0.23:51522` | NULL |
| 21 E | 10:17:10.000 | 10:16:40.000 | 30000 | dn3 | c04d88-8 | carol | read | client | remote | `BP-1` | 1073741901 | 1073741901 | 100663296 | 0 | false | `/10.0.0.22:43120` | NULL |
| 22 E | 10:17:20.500 | 10:16:40.000 | 40500 | dn3 | c04d88-8 | carol | read | client | remote | `BP-1` | 1073741901 | 1073741901 | 33554432 | 0 | true | `/10.0.0.22:43120` | NULL |

Notes:

- 22 events are 18 operations: operation A on each DataNode and read E have two events each.
- The replica on the last DataNode of a pipeline closes first, so the events of one write go
  from the last DataNode of its pipeline to the first: 1–3, 8–10, 11–13, 15–17, 18–20.
- `file_block_id` equals `block_id`: the blocks are not erasure-coded. For striped blocks
  `file_block_id` would hold the block group id, which `ResolvePaths` uses to find the file.
- `scr_fds` (event 14) hands out file descriptors for a short-circuit read. The client reads
  the local disk itself, so `bytes = 0`, `est_bytes` holds the block length, and `dur_ms = 2`
  is the time to hand out the descriptors, not the read time.

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
| 16393 | D | `external` |
| 16394 | D | `hive` |
| 16400 | D | `sales.db` |
| 16410 | D | `orders` |
| 16415 | D | `dt=2026-11-05` |
| 16418 | D | `delta_0000012_0000012` |
| 16420 | F | `bucket_00000` |
| 16500 | D | `landing` |
| 16505 | D | `crm` |
| 16512 | F | `clients.csv` |
| 16600 | D | `radcom.db` |
| 16610 | D | `cdr` |
| 16620 | D | `gtpu` |
| 16621 | D | `dt=2026-11-06` |
| 16622 | D | `hour=10` |
| 16625 | F | `000000_0` |
| 16630 | D | `s1ap` |
| 16631 | D | `dt=2026-11-06` |
| 16632 | D | `hour=10` |
| 16635 | F | `000000_0` |

`fs_edge` (current parent):

| child_id | parent_id |
|---|---|
| 16386 | 16385 |
| 16390 | 16386 |
| 16391 | 16390 |
| 16392 | 16391 |
| 16393 | 16390 |
| 16394 | 16393 |
| 16400 | 16392 |
| 16410 | 16400 |
| 16415 | 16410 |
| 16418 | 16415 |
| 16420 | 16418 |
| 16500 | 16385 |
| 16505 | 16500 |
| 16512 | 16505 |
| 16600 | 16394 |
| 16610 | 16600 |
| 16620 | 16610 |
| 16621 | 16620 |
| 16622 | 16621 |
| 16625 | 16622 |
| 16630 | 16610 |
| 16631 | 16630 |
| 16632 | 16631 |
| 16635 | 16632 |

`fs_block_inode`:

| block_id | inode_id |
|---|---|
| 1073741901 | 16420 |
| 1073741955 | 16512 |
| 1073742100 | 16625 |
| 1073742120 | 16635 |

Block `1073742010` is not in the image: the file under `/tmp/hive/alice/…` was deleted before
the checkpoint. `clients.csv` is already under its final name: `hdfs dfs -put` renamed
`clients.csv._COPYING_` after the write. The `dt=2026-11-06/hour=10` partitions of `gtpu` and
`s1ap` were created on 2026-11-06 and are in the image because it was taken after that day ended.

`fs_meta`:

| txid | image_file | parsed_at |
|---|---|---|
| 4815162 | `fsimage_0000000000004815162` | 1794018720000 |

## Step 4. `event_path` (ResolvePaths)

`ResolvePaths --dt 2026-11-06 --image-dt 2026-11-07` does the following:

1. Takes the `file_block_id` of the day's events, except `-1`, and looks up their `inode_id`
   in `fs_block_inode`.
2. Climbs `fs_edge` to the root (16385) and joins the names from `fs_inode` into a path.
3. Fills `path`: the event's path if it has one (the WebHDFS HTTP leg), otherwise the path
   from the image, otherwise `?`.
4. Computes `table_dir` from `path`: drops the file name and cuts at the first component of the
   form `key=value`, `.x`, `_x`, `base_N`, `delta_N_N` or `delete_delta_N_N`.
5. Computes `peer_ip` from `peer`: the address without the port and without a host name before
   `/`. Only for `locality = remote`; local events and domain sockets get NULL.

All `events` columns are carried over unchanged except `path`; `inode_id`, `table_dir` and
`peer_ip` are added. The columns `start`, `bpid` and `block_id` are not shown below: they are
the same as in step 2 for the row with the same `#`.

| # | ts | dn | op_id | user | op | role | locality | file_block_id | dur_ms | bytes | est_bytes | last | peer | path | inode_id | table_dir | peer_ip |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 C | 10:15:39.930 | dn3 | c04d88-6 | etl | write | pipeline | remote | 1073741955 | 3710 | 10485760 | 0 | true | `/10.0.0.12:38900` | `/landing/crm/clients.csv` | 16512 | `/landing/crm` | 10.0.0.12 |
| 2 C | 10:15:39.940 | dn2 | 7be019-41 | etl | write | pipeline | remote | 1073741955 | 3730 | 10485760 | 0 | true | `/10.0.0.11:47790` | `/landing/crm/clients.csv` | 16512 | `/landing/crm` | 10.0.0.11 |
| 3 C | 10:15:39.950 | dn1 | a1f3c2-19 | etl | write | webhdfs_proxy | local | 1073741955 | 3750 | 10485760 | 0 | true | `/10.0.0.11:51310` | `/landing/crm/clients.csv` | 16512 | `/landing/crm` | NULL |
| 4 C | 10:15:40.000 | dn1 | a1f3c2-18 | etl | webhdfs_create | client | remote | -1 | 4000 | 10485760 | 0 | true | `/10.0.0.50:41234` | `/landing/crm/clients.csv._COPYING_` | NULL | `/landing/crm` | 10.0.0.50 |
| 5 A | 10:15:50.000 | dn1 | a1f3c2-17 | alice | write | client | local | 1073741901 | 30000 | 83886080 | 0 | false | `/10.0.0.11:51234` | `/warehouse/tablespace/managed/hive/sales.db/orders/dt=2026-11-05/delta_0000012_0000012/bucket_00000` | 16420 | `/warehouse/tablespace/managed/hive/sales.db/orders` | NULL |
| 6 A | 10:15:50.010 | dn2 | 7be019-40 | alice | write | pipeline | remote | 1073741901 | 30000 | 83886080 | 0 | false | `/10.0.0.11:47710` | `/warehouse/tablespace/managed/hive/sales.db/orders/dt=2026-11-05/delta_0000012_0000012/bucket_00000` | 16420 | `/warehouse/tablespace/managed/hive/sales.db/orders` | 10.0.0.11 |
| 7 A | 10:15:50.020 | dn3 | c04d88-5 | alice | write | pipeline | remote | 1073741901 | 30000 | 83886080 | 0 | false | `/10.0.0.12:38822` | `/warehouse/tablespace/managed/hive/sales.db/orders/dt=2026-11-05/delta_0000012_0000012/bucket_00000` | 16420 | `/warehouse/tablespace/managed/hive/sales.db/orders` | 10.0.0.12 |
| 8 D | 10:15:54.980 | dn2 | 7be019-42 | alice | write | pipeline | remote | 1073742010 | 960 | 2097152 | 0 | true | `/10.0.0.11:47850` | `?` | NULL | `?` | 10.0.0.11 |
| 9 D | 10:15:54.990 | dn1 | a1f3c2-20 | alice | write | pipeline | remote | 1073742010 | 980 | 2097152 | 0 | true | `/10.0.0.13:52004` | `?` | NULL | `?` | 10.0.0.13 |
| 10 D | 10:15:55.000 | dn3 | c04d88-7 | alice | write | client | remote | 1073742010 | 1000 | 2097152 | 0 | true | `/10.0.0.21:60112` | `?` | NULL | `?` | 10.0.0.21 |
| 11 A | 10:16:11.980 | dn3 | c04d88-5 | alice | write | pipeline | remote | 1073741901 | 51960 | 50331648 | 0 | true | `/10.0.0.12:38822` | `/warehouse/tablespace/managed/hive/sales.db/orders/dt=2026-11-05/delta_0000012_0000012/bucket_00000` | 16420 | `/warehouse/tablespace/managed/hive/sales.db/orders` | 10.0.0.12 |
| 12 A | 10:16:11.990 | dn2 | 7be019-40 | alice | write | pipeline | remote | 1073741901 | 51980 | 50331648 | 0 | true | `/10.0.0.11:47710` | `/warehouse/tablespace/managed/hive/sales.db/orders/dt=2026-11-05/delta_0000012_0000012/bucket_00000` | 16420 | `/warehouse/tablespace/managed/hive/sales.db/orders` | 10.0.0.11 |
| 13 A | 10:16:12.000 | dn1 | a1f3c2-17 | alice | write | client | local | 1073741901 | 52000 | 50331648 | 0 | true | `/10.0.0.11:51234` | `/warehouse/tablespace/managed/hive/sales.db/orders/dt=2026-11-05/delta_0000012_0000012/bucket_00000` | 16420 | `/warehouse/tablespace/managed/hive/sales.db/orders` | NULL |
| 14 B | 10:16:30.002 | dn2 | 7be019-43 | bob | scr_fds | client | local | 1073741901 | 2 | 0 | 134217728 | true | `unix:/var/lib/hadoop-hdfs/dn_socket` | `/warehouse/tablespace/managed/hive/sales.db/orders/dt=2026-11-05/delta_0000012_0000012/bucket_00000` | 16420 | `/warehouse/tablespace/managed/hive/sales.db/orders` | NULL |
| 15 F | 10:16:56.980 | dn1 | a1f3c2-21 | radcom | write | pipeline | remote | 1073742100 | 11960 | 67108864 | 0 | true | `/10.0.0.13:52080` | `/warehouse/tablespace/external/hive/radcom.db/cdr/gtpu/dt=2026-11-06/hour=10/000000_0` | 16625 | `/warehouse/tablespace/external/hive/radcom.db/cdr/gtpu` | 10.0.0.13 |
| 16 F | 10:16:56.990 | dn3 | c04d88-9 | radcom | write | pipeline | remote | 1073742100 | 11980 | 67108864 | 0 | true | `/10.0.0.12:38950` | `/warehouse/tablespace/external/hive/radcom.db/cdr/gtpu/dt=2026-11-06/hour=10/000000_0` | 16625 | `/warehouse/tablespace/external/hive/radcom.db/cdr/gtpu` | 10.0.0.12 |
| 17 F | 10:16:57.000 | dn2 | 7be019-44 | radcom | write | client | local | 1073742100 | 12000 | 67108864 | 0 | true | `/10.0.0.12:52310` | `/warehouse/tablespace/external/hive/radcom.db/cdr/gtpu/dt=2026-11-06/hour=10/000000_0` | 16625 | `/warehouse/tablespace/external/hive/radcom.db/cdr/gtpu` | NULL |
| 18 G | 10:17:08.980 | dn2 | 7be019-45 | radcom | write | pipeline | remote | 1073742120 | 3960 | 16777216 | 0 | true | `/10.0.0.13:38990` | `/warehouse/tablespace/external/hive/radcom.db/cdr/s1ap/dt=2026-11-06/hour=10/000000_0` | 16635 | `/warehouse/tablespace/external/hive/radcom.db/cdr/s1ap` | 10.0.0.13 |
| 19 G | 10:17:08.990 | dn3 | c04d88-10 | radcom | write | pipeline | remote | 1073742120 | 3980 | 16777216 | 0 | true | `/10.0.0.11:52120` | `/warehouse/tablespace/external/hive/radcom.db/cdr/s1ap/dt=2026-11-06/hour=10/000000_0` | 16635 | `/warehouse/tablespace/external/hive/radcom.db/cdr/s1ap` | 10.0.0.11 |
| 20 G | 10:17:09.000 | dn1 | a1f3c2-22 | radcom | write | client | remote | 1073742120 | 4000 | 16777216 | 0 | true | `/10.0.0.23:51522` | `/warehouse/tablespace/external/hive/radcom.db/cdr/s1ap/dt=2026-11-06/hour=10/000000_0` | 16635 | `/warehouse/tablespace/external/hive/radcom.db/cdr/s1ap` | 10.0.0.23 |
| 21 E | 10:17:10.000 | dn3 | c04d88-8 | carol | read | client | remote | 1073741901 | 30000 | 100663296 | 0 | false | `/10.0.0.22:43120` | `/warehouse/tablespace/managed/hive/sales.db/orders/dt=2026-11-05/delta_0000012_0000012/bucket_00000` | 16420 | `/warehouse/tablespace/managed/hive/sales.db/orders` | 10.0.0.22 |
| 22 E | 10:17:20.500 | dn3 | c04d88-8 | carol | read | client | remote | 1073741901 | 40500 | 33554432 | 0 | true | `/10.0.0.22:43120` | `/warehouse/tablespace/managed/hive/sales.db/orders/dt=2026-11-05/delta_0000012_0000012/bucket_00000` | 16420 | `/warehouse/tablespace/managed/hive/sales.db/orders` | 10.0.0.22 |

Notes:

- Event 4 (the WebHDFS HTTP leg) has no block (`file_block_id = -1`), so `inode_id = NULL` and
  `path` stays as in the event, with the temporary name `._COPYING_`. Events 1–3 of the same
  upload get the final name from the image by block. All four get the same `table_dir`
  because the file name is dropped.
- For `orders` the cut happens at `dt=2026-11-05`. For an unpartitioned table it would happen
  at `delta_0000012_0000012`, with the same `table_dir`.
- Events 8–10 got `?`: the block is not in `fs_block_inode` and the events have no `path`. The
  final `ResolvePaths` line for this day: `events=22 unresolved=3`.
- `peer_ip` shows which address the data came from. For client events it is the client host
  (10.0.0.50 for `etl`, 10.0.0.21 and 10.0.0.23 for the Hive tasks, 10.0.0.22 for `carol`); for
  replicas it is the previous DataNode of the pipeline. Events 3, 5, 13 and 17 have the client
  on the same host and event 14 is a domain socket, so they get NULL.
- `cdr/gtpu` and `cdr/s1ap` get different `table_dir` values: the cut happens at the first
  `key=value` component (`dt=2026-11-06`), and `gtpu` and `s1ap` are above it. `TableDir` does
  not consult the Hive metastore, so the result is the same even if the metastore has one
  table `cdr` with `LOCATION` `/warehouse/tablespace/external/hive/radcom.db/cdr` and `gtpu` and
  `s1ap` are its subdirectories.

## Step 5. `traffic_minute` (ResolvePaths)

`event_path` is grouped by `minute, user, op, role, locality, table_dir`. `minute` is the start
of the `ts` minute in epoch ms; `bytes` and `est_bytes` are summed; `ops` counts events with
`last = true`. `busy_ms` is the sum of `dur_ms` growth since the previous event of the same
operation (`dn`, `op_id`); for the first event of an operation the growth is `dur_ms`.
`origin` is computed with `--nets own=10.0.0.0/24`: events with `locality = local` get
`local`, all other addresses of the example are in 10.0.0.0/24 and get `own`. The
"rows" column is not part of the table: it shows which events `#` a group is
built from.

| minute | user | op | role | locality | table_dir | bytes | est_bytes | ops | busy_ms | origin | rows |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1793960100000 (10:15) | alice | write | client | local | `/warehouse/tablespace/managed/hive/sales.db/orders` | 83886080 | 0 | 0 | 30000 | local | 5 |
| 1793960100000 (10:15) | alice | write | client | remote | `?` | 2097152 | 0 | 1 | 1000 | own | 10 |
| 1793960100000 (10:15) | alice | write | pipeline | remote | `/warehouse/tablespace/managed/hive/sales.db/orders` | 167772160 | 0 | 0 | 60000 | own | 6, 7 |
| 1793960100000 (10:15) | alice | write | pipeline | remote | `?` | 4194304 | 0 | 2 | 1940 | own | 8, 9 |
| 1793960100000 (10:15) | etl | webhdfs_create | client | remote | `/landing/crm` | 10485760 | 0 | 1 | 4000 | own | 4 |
| 1793960100000 (10:15) | etl | write | pipeline | remote | `/landing/crm` | 20971520 | 0 | 2 | 7440 | own | 1, 2 |
| 1793960100000 (10:15) | etl | write | webhdfs_proxy | local | `/landing/crm` | 10485760 | 0 | 1 | 3750 | local | 3 |
| 1793960160000 (10:16) | alice | write | client | local | `/warehouse/tablespace/managed/hive/sales.db/orders` | 50331648 | 0 | 1 | 22000 | local | 13 |
| 1793960160000 (10:16) | alice | write | pipeline | remote | `/warehouse/tablespace/managed/hive/sales.db/orders` | 100663296 | 0 | 2 | 43940 | own | 11, 12 |
| 1793960160000 (10:16) | bob | scr_fds | client | local | `/warehouse/tablespace/managed/hive/sales.db/orders` | 0 | 134217728 | 1 | 2 | local | 14 |
| 1793960160000 (10:16) | radcom | write | client | local | `/warehouse/tablespace/external/hive/radcom.db/cdr/gtpu` | 67108864 | 0 | 1 | 12000 | local | 17 |
| 1793960160000 (10:16) | radcom | write | pipeline | remote | `/warehouse/tablespace/external/hive/radcom.db/cdr/gtpu` | 134217728 | 0 | 2 | 23940 | own | 15, 16 |
| 1793960220000 (10:17) | carol | read | client | remote | `/warehouse/tablespace/managed/hive/sales.db/orders` | 134217728 | 0 | 1 | 40500 | own | 21, 22 |
| 1793960220000 (10:17) | radcom | write | client | remote | `/warehouse/tablespace/external/hive/radcom.db/cdr/s1ap` | 16777216 | 0 | 1 | 4000 | own | 20 |
| 1793960220000 (10:17) | radcom | write | pipeline | remote | `/warehouse/tablespace/external/hive/radcom.db/cdr/s1ap` | 33554432 | 0 | 2 | 7940 | own | 18, 19 |

Notes:

- The bytes of operation A are split between two minutes by event emission time: 80 MiB on
  each DataNode at 10:15, 48 MiB at 10:16. Each of the three A operations is counted in `ops`
  once, in the minute of its last event.
- Both events of read E fall into 10:17 and form one group: 128 MiB, `ops = 1`. The `orders`
  block is read twice: over the network (`read`, `bytes`) and via short-circuit (`scr_fds`,
  `est_bytes` only).
- The same 128 MiB of `alice` add up to 384 MiB over all roles: `client` on dn1 and two
  `pipeline` replicas. The 10 MiB upload of `etl` adds up to 40 MiB: the HTTP leg,
  `webhdfs_proxy` and two replicas.
- The traffic of both `cdr` directories is summed by prefix: `table_dir LIKE
  '/warehouse/tablespace/external/hive/radcom.db/cdr/%'`. At 10:16 and 10:17 these are the
  groups of events 15–17 and 18–20.
- `busy_ms` of operation A on dn1: 30000 at 10:15 (first event, `dur_ms` = 30000) and 22000
  at 10:16 (52000 − 30000). Together this is the full duration of the operation, 52 s.
- `busy_ms` belongs to the minute of the event, like `bytes`, not to the minutes in which the
  operation actually ran. Read E started at 10:16:40, but all 40500 ms fall into 10:17: its
  first event was emitted at 10:17:10.
- `busy_ms` of different operations add up: the three A replicas have 60000 ms together at
  10:15 within one minute. It is the total time of operations, not a share of the minute.
- An operation started before 00:00 UTC of day `dt` counts only the time after midnight in its
  first event of the day: the part before midnight is already counted in the previous day's
  partition.
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

| minute | table_dir | mib_per_s | rows |
|---|---|---|---|
| 10:15 | `/warehouse/tablespace/managed/hive/sales.db/orders` | 1.333 | 5 |
| 10:15 | `/landing/crm` | 0.167 | 4 |
| 10:15 | `?` | 0.033 | 10 |
| 10:16 | `/warehouse/tablespace/external/hive/radcom.db/cdr/gtpu` | 1.067 | 17 |
| 10:16 | `/warehouse/tablespace/managed/hive/sales.db/orders` | 0.8 | 13 |
| 10:17 | `/warehouse/tablespace/external/hive/radcom.db/cdr/s1ap` | 0.267 | 20 |

Read throughput per table per minute, MiB/s. Short-circuit reads are not included: `scr_fds`
has bytes only in `est_bytes`:

```sql
SELECT minute, table_dir, SUM(bytes) / 1048576 / 60 AS mib_per_s
FROM hdfs_traffic.traffic_minute
WHERE dt = '2026-11-06'
  AND op IN ('read', 'webhdfs_read') AND role = 'client'
GROUP BY minute, table_dir
ORDER BY minute, mib_per_s DESC;
```

Result on the example data:

| minute | table_dir | mib_per_s | rows |
|---|---|---|---|
| 10:17 | `/warehouse/tablespace/managed/hive/sales.db/orders` | 2.133 | 21, 22 |

Average speed of one operation per table per minute, MiB/s. The divisor is `busy_ms`, not
60 s, so the value does not depend on how many operations ran in parallel:

```sql
SELECT minute, table_dir, SUM(bytes) / 1048576 / (SUM(busy_ms) / 1000) AS mib_per_s
FROM hdfs_traffic.traffic_minute
WHERE dt = '2026-11-06'
  AND op IN ('write', 'webhdfs_create', 'webhdfs_append') AND role = 'client'
GROUP BY minute, table_dir
HAVING SUM(busy_ms) > 0
ORDER BY minute, mib_per_s DESC;
```

Result on the example data:

| minute | table_dir | mib_per_s | rows |
|---|---|---|---|
| 10:15 | `/warehouse/tablespace/managed/hive/sales.db/orders` | 2.667 | 5 |
| 10:15 | `/landing/crm` | 2.5 | 4 |
| 10:15 | `?` | 2 | 10 |
| 10:16 | `/warehouse/tablespace/external/hive/radcom.db/cdr/gtpu` | 5.333 | 17 |
| 10:16 | `/warehouse/tablespace/managed/hive/sales.db/orders` | 2.182 | 13 |
| 10:17 | `/warehouse/tablespace/external/hive/radcom.db/cdr/s1ap` | 4 | 20 |

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

Result on the example data (18 operations):

| dn | op_id | user | op | role | table_dir | bytes | dur_ms | mib_per_s |
|---|---|---|---|---|---|---|---|---|
| dn3 | c04d88-6 | etl | write | pipeline | `/landing/crm` | 10485760 | 3710 | 2.695 |
| dn2 | 7be019-41 | etl | write | pipeline | `/landing/crm` | 10485760 | 3730 | 2.681 |
| dn1 | a1f3c2-19 | etl | write | webhdfs_proxy | `/landing/crm` | 10485760 | 3750 | 2.667 |
| dn1 | a1f3c2-18 | etl | webhdfs_create | client | `/landing/crm` | 10485760 | 4000 | 2.5 |
| dn1 | a1f3c2-17 | alice | write | client | `/warehouse/tablespace/managed/hive/sales.db/orders` | 134217728 | 52000 | 2.462 |
| dn2 | 7be019-40 | alice | write | pipeline | `/warehouse/tablespace/managed/hive/sales.db/orders` | 134217728 | 51980 | 2.462 |
| dn3 | c04d88-5 | alice | write | pipeline | `/warehouse/tablespace/managed/hive/sales.db/orders` | 134217728 | 51960 | 2.463 |
| dn2 | 7be019-42 | alice | write | pipeline | `?` | 2097152 | 960 | 2.083 |
| dn1 | a1f3c2-20 | alice | write | pipeline | `?` | 2097152 | 980 | 2.041 |
| dn3 | c04d88-7 | alice | write | client | `?` | 2097152 | 1000 | 2.0 |
| dn2 | 7be019-43 | bob | scr_fds | client | `/warehouse/tablespace/managed/hive/sales.db/orders` | 0 | 2 | 0.0 |
| dn1 | a1f3c2-21 | radcom | write | pipeline | `/warehouse/tablespace/external/hive/radcom.db/cdr/gtpu` | 67108864 | 11960 | 5.351 |
| dn3 | c04d88-9 | radcom | write | pipeline | `/warehouse/tablespace/external/hive/radcom.db/cdr/gtpu` | 67108864 | 11980 | 5.342 |
| dn2 | 7be019-44 | radcom | write | client | `/warehouse/tablespace/external/hive/radcom.db/cdr/gtpu` | 67108864 | 12000 | 5.333 |
| dn2 | 7be019-45 | radcom | write | pipeline | `/warehouse/tablespace/external/hive/radcom.db/cdr/s1ap` | 16777216 | 3960 | 4.04 |
| dn3 | c04d88-10 | radcom | write | pipeline | `/warehouse/tablespace/external/hive/radcom.db/cdr/s1ap` | 16777216 | 3980 | 4.02 |
| dn1 | a1f3c2-22 | radcom | write | client | `/warehouse/tablespace/external/hive/radcom.db/cdr/s1ap` | 16777216 | 4000 | 4.0 |
| dn3 | c04d88-8 | carol | read | client | `/warehouse/tablespace/managed/hive/sales.db/orders` | 134217728 | 40500 | 3.16 |

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
  write with replication 3 is counted three times, a WebHDFS `put` with replication 3 four
  times.
