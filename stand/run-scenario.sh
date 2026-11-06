#!/usr/bin/env bash
# Drive known traffic through the running stand and check the agent's counters.
#   ./run-scenario.sh hdp|apache
# Each step uses its own user, so every counter has one source and an exact expected value.
set -euo pipefail

STAND_DIR=$(cd "$(dirname "$0")" && pwd)
FLAVOR=${1:-hdp}
REALM=TRAFFIC.LOCAL
WORK=$(mktemp -d)
trap 'rm -rf "${WORK}"' EXIT

step() { echo; echo "== $*"; }

# Run a shell snippet in a container with a Kerberos ticket of the given user.
as_user() {
  local container=$1 user=$2
  shift 2
  docker exec -e KRB5CCNAME="/tmp/krb5cc_${user}" -e HADOOP_ROOT_LOGGER=ERROR,console "${container}" bash -c \
    "kinit -kt /keytabs/${user}.keytab ${user}@${REALM} && $*"
}

as_hdfs() {
  docker exec -e HADOOP_ROOT_LOGGER=ERROR,console tr-nn runuser -u hdfs -- bash -c \
    "kinit -kt /etc/security/keytabs/host.keytab hdfs/tr-nn.traffic-net@${REALM} && $*"
}

# Agent log lines, whatever log4j layout the distribution ships.
agent_log() {
  docker logs "$1" 2>&1 | grep -E '(INFO|WARN|ERROR).*hdfs-traffic-agent( |:)' | grep -v classpath || true
}

step "agent on the DataNodes"
for dn in tr-dn1 tr-dn2 tr-dn3; do
  if ! agent_log "${dn}" | grep -q 'UserTraffic metrics source registered'; then
    agent_log "${dn}" >&2
    echo "FAIL: agent metrics source is not registered on ${dn}" >&2
    exit 1
  fi
  echo "${dn}: agent installed, UserTraffic registered"
done
step "cluster: 3 live DataNodes, /traffic"
for _ in $(seq 1 60); do
  live=$(as_hdfs 'hdfs dfsadmin -report -live 2>/dev/null' | sed -n 's/^Live datanodes (\([0-9]*\)).*/\1/p')
  [[ "${live}" == "3" ]] && break
  sleep 2
done
[[ "${live}" == "3" ]] || { echo "FAIL: live DataNodes: ${live:-0}" >&2; exit 1; }
as_hdfs 'hdfs dfsadmin -safemode wait >/dev/null && hdfs dfs -mkdir -p /traffic && hdfs dfs -chmod 1777 /traffic'

python3 -I "${STAND_DIR}/check_traffic.py" snapshot > "${WORK}/baseline.json"
now_ms() { python3 -c 'import time; print(int(time.time() * 1000))'; }
since_ms=$(now_ms)

docker exec tr-client bash -c \
  'head -c 3145728 /dev/urandom > /tmp/3m && head -c 2097152 /dev/urandom > /tmp/2m && head -c 1048576 /dev/urandom > /tmp/1m'

step "alice writes 3 MiB, replication 3 (client + 2 pipeline hops)"
as_user tr-client alice 'hdfs dfs -put -f /tmp/3m /traffic/alice.bin'

step "dave writes 1 MiB with replication 1, then setrep 3 (NameNode-driven re-replication)"
as_user tr-client dave 'hdfs dfs -D dfs.replication=1 -put -f /tmp/1m /traffic/dave.bin && hdfs dfs -setrep -w 3 /traffic/dave.bin'

step "bob reads alice's file from the client container (remote)"
as_user tr-client bob 'hdfs dfs -cat /traffic/alice.bin | wc -c'

step "carol reads it on tr-dn1 itself (local, short-circuit off)"
as_user tr-dn1 carol 'hdfs dfs -cat /traffic/alice.bin | wc -c'

step "bob writes and reads 2 MiB over WebHDFS"
as_user tr-client bob 'hdfs dfs -put -f /tmp/2m webhdfs://tr-nn:9870/traffic/bob-web.bin && hdfs dfs -cat webhdfs://tr-nn:9870/traffic/bob-web.bin | wc -c'

# Classes are instrumented as the JVM loads them; the WebHDFS ones only on the DataNode that
# served the HTTP request. Hence the union over all DataNodes, after the traffic.
step "agent: intercepted classes instrumented (union over DataNodes)"
classes=$(for dn in tr-dn1 tr-dn2 tr-dn3; do agent_log "${dn}"; done \
  | sed -n 's/.*hdfs-traffic-agent: instrumented //p' | sort -u)
echo "${classes}" | sed 's/^/  /'
if [[ $(echo "${classes}" | grep -c .) -lt 8 ]] \
    || for dn in tr-dn1 tr-dn2 tr-dn3; do agent_log "${dn}"; done | grep -qE 'WARN|failed to instrument'; then
  for dn in tr-dn1 tr-dn2 tr-dn3; do agent_log "${dn}"; done | grep -E 'WARN|failed' >&2 || true
  echo "FAIL: expected 8 instrumented classes and no agent warnings" >&2
  exit 1
fi

step "UserTraffic delta, summed over the DataNodes"
python3 -I "${STAND_DIR}/check_traffic.py" verify "${WORK}/baseline.json"

until_ms=$(now_ms)

# Events: every DataNode ships its closed files (rotation every 20 s) into HDFS; compare the
# events of this run with the same expectations. Retry while files are still being rotated.
step "events: shipped to HDFS, summed over the DataNodes"
ok=""
for attempt in 1 2 3 4 5; do
  sleep 15
  for dn in tr-dn1 tr-dn2 tr-dn3; do
    docker exec "${dn}" runuser -u hdfs -- /opt/hdfs-traffic-deploy/upload-events.sh \
      -d /var/log/hdfs-traffic -r /data/hdfs-traffic/raw \
      -k /etc/security/keytabs/host.keytab -p "hdfs/${dn}.traffic-net@${REALM}" >/dev/null
  done
  if as_hdfs 'hdfs dfs -cat "/data/hdfs-traffic/raw/dt=*/dn=*/*.jsonl"' \
      | python3 -I "${STAND_DIR}/check_traffic.py" events "${since_ms}" "${until_ms}" \
      > "${WORK}/events.txt"; then
    ok=1
    break
  fi
done
cat "${WORK}/events.txt"
[[ -n "${ok}" ]] || { echo "FAIL: events in HDFS do not match the scenario" >&2; exit 1; }
