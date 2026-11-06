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
[-k <keytab> -p <principal>] [-H <host>]` puts closed files into HDFS as
`<root>/dt=YYYY-MM-DD/dn=<host>/<file>` and deletes the local file only after a successful
put. It takes a `flock`, so runs never overlap, and uses its own Kerberos credential cache
in the events directory. The partition `dn=` is the node key: `hostname -f` by default,
`-H` overrides it; it may differ from the `dn` field inside the events. One failing file
does not stop the run: the others are still shipped and the exit code is 1.

Copy `deploy/upload-events.sh` to the DataNode (e.g. next to the agent jar,
`/opt/hdfs-traffic-agent/`) and run it from cron, for example:

```
*/5 * * * * hdfs /opt/hdfs-traffic-agent/upload-events.sh -d /var/log/hadoop/hdfs-traffic \
    -r /data/hdfs-traffic/raw -k /etc/security/keytabs/hdfs.headless.keytab -p hdfs@REALM
```

The upload's own writes into HDFS appear as the service user's traffic.

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
