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

The jar is `target/hdfs-traffic-agent-<version>.jar` (same jar for both Hadoop lines).

## Install

1. Copy the jar to every DataNode, e.g. `/opt/hdfs-traffic-agent/hdfs-traffic-agent.jar`.
2. Add to `hadoop-env.sh` (HDP: Ambari → HDFS → Configs → Advanced hadoop-env template):

   ```bash
   export HDFS_DATANODE_OPTS="$HDFS_DATANODE_OPTS -javaagent:/opt/hdfs-traffic-agent/hdfs-traffic-agent.jar=maxUsers=2000"
   ```

   `HADOOP_DATANODE_OPTS` (deprecated name) is honored by Hadoop 3 as well.

   Agent options: the only option is `maxUsers` (default 2000, must be ≥ 1). User slots
   are taken first-come since DataNode start; later users go to `__other__`. An unknown or
   invalid option disables the agent (WARN in the DataNode log), but the DataNode still
   starts.
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

## Docker stand

`stand/` runs the agent on a real secure cluster: MIT KDC, a NameNode, three DataNodes
started with `-javaagent`, and a client container. Kerberos, block access tokens and SASL
data transfer protection are on, as in production. Two distributions are supported: HDP
3.1.0.0-78 (from a local copy, the jars are not redistributable) and Apache Hadoop 3.3.6
(copied out of the `apache/hadoop:3.3.6` image).

```bash
mvn clean package -P hadoop-3.1.1      # JDK 8; the stand mounts target/*.jar
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
exact expected byte counts. DataNode `/jmx` is published on host ports 29864–29866; the
NameNode UI (SPNEGO) is on 29870.

## Limitations

- Bytes of a block write are reported when the write operation ends (block closed), so
  long-open blocks (HBase WAL, streaming writers) appear as a spike at close; in-flight
  bytes are lost if the DataNode restarts.
- Exact bytes of short-circuit reads are not visible (see `EstimatedBytes`: block length
  once per descriptor request; later reads served from the client's descriptor cache are
  not counted). WebHDFS reads of local replicas may use short-circuit inside the DataNode
  and then appear as `op=scr_fds, role=client` instead of `webhdfs_proxy`.
- Reads: positional reads not aligned to checksum chunks can count up to one chunk (512 B
  by default) more than requested.
- Bytes of a write packet that fails mid-receive are not counted.
- A WebHDFS stream that is never closed loses its event.
- DataTransferProtocol locality uses the socket's own addresses (client connected to the
  same IP it came from); a local client using a different local IP of a multi-homed host
  is counted as `remote`.
- Block-to-file mapping is not implemented yet (events carry block pool id and block id).
