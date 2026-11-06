#!/usr/bin/env bash
# Ship closed hdfs-traffic-agent event files from this DataNode into HDFS, then delete them.
# Run every 5 minutes as the user that owns the events directory. The recommended runner is
# the systemd timer in deploy/systemd/ (randomized start, so DataNodes do not hit the
# NameNode and the KDC in the same second). Where the timer cannot randomize (systemd < 229,
# e.g. RHEL/CentOS 7) or from cron, -j <sec> sleeps a random 0..sec seconds first:
#   */5 * * * * hdfs /opt/hdfs-traffic-agent/upload-events.sh -j 240 \
#       -d /var/log/hadoop/hdfs-traffic -r /data/hdfs-traffic/raw \
#       -k /etc/security/keytabs/hdfs.headless.keytab -p hdfs@REALM
# Files are shipped in batches: one "hdfs dfs -mkdir" for all target directories and one
# "hdfs dfs -put" per target directory and batch (-b files), instead of a JVM per file.
# Closed files are immutable and carry the UTC date of their events in the name, so a re-run
# after a failure simply puts the same files again (-f). A local file is deleted only after
# its whole batch was put; a failing batch does not stop the others and the exit code is 1.
# The partition dn=<host> is the node key (default: hostname -f, override with -H); it may
# differ from the "dn" field inside the events.
set -euo pipefail

EVENTS_DIR=/var/log/hadoop/hdfs-traffic
ROOT=/data/hdfs-traffic/raw
PREFIX=events
KEYTAB=
PRINCIPAL=
HOST=
BATCH=100
JITTER=0

usage() {
  echo "usage: $0 [-d events_dir] [-r hdfs_root] [-n file_prefix] [-k keytab -p principal]" \
    "[-H host] [-b batch_files] [-j max_jitter_sec]" >&2
  exit 2
}

while getopts "d:r:n:k:p:H:b:j:" opt; do
  case "${opt}" in
    d) EVENTS_DIR=${OPTARG} ;;
    r) ROOT=${OPTARG} ;;
    n) PREFIX=${OPTARG} ;;
    k) KEYTAB=${OPTARG} ;;
    p) PRINCIPAL=${OPTARG} ;;
    H) HOST=${OPTARG} ;;
    b) BATCH=${OPTARG} ;;
    j) JITTER=${OPTARG} ;;
    *) usage ;;
  esac
done
if [[ -n "${KEYTAB}" && -z "${PRINCIPAL}" ]]; then
  usage
fi
if ! [[ "${BATCH}" =~ ^[1-9][0-9]*$ && "${JITTER}" =~ ^[0-9]+$ ]]; then
  usage
fi

if (( JITTER > 0 )); then
  sleep $(( (RANDOM * 32768 + RANDOM) % (JITTER + 1) ))
fi

if [[ ! -d "${EVENTS_DIR}" ]]; then
  echo "upload-events: ${EVENTS_DIR} does not exist, nothing to ship"
  exit 0
fi

exec 9>"${EVENTS_DIR}/.upload.lock"
if ! flock -n 9; then
  echo "upload-events: another run is in progress" >&2
  exit 0
fi

# Closed files only; the glob sorts by name, i.e. by UTC time, so each day is contiguous.
shopt -s nullglob
files=("${EVENTS_DIR}/${PREFIX}"-[0-9]*-[0-9]*-[0-9]*.jsonl)
if (( ${#files[@]} == 0 )); then
  exit 0 # nothing to ship: no kinit, no JVM
fi

if [[ -n "${KEYTAB}" ]]; then
  export KRB5CCNAME="FILE:${EVENTS_DIR}/.upload.krb5cc"
  kinit -kt "${KEYTAB}" "${PRINCIPAL}"
fi

host=${HOST:-$(hostname -f)}

dest_of() {
  local name stamp day
  name=$(basename "$1")
  stamp=${name#"${PREFIX}"-}
  day=${stamp:0:8}
  echo "${ROOT}/dt=${day:0:4}-${day:4:2}-${day:6:2}/dn=${host}"
}

dests=()
last=
for f in "${files[@]}"; do
  d=$(dest_of "${f}")
  if [[ "${d}" != "${last}" ]]; then
    dests+=("${d}")
    last=${d}
  fi
done
if ! hdfs dfs -mkdir -p "${dests[@]}"; then
  echo "upload-events: cannot create ${dests[*]}" >&2
  exit 1
fi

rc=0
batch=()
batch_dest=

ship() {
  (( ${#batch[@]} > 0 )) || return 0
  if hdfs dfs -put -f "${batch[@]}" "${batch_dest}/" && rm -f "${batch[@]}"; then
    local f
    for f in "${batch[@]}"; do
      echo "upload-events: $(basename "${f}") -> ${batch_dest}"
    done
  else
    rc=1
    echo "upload-events: failed ${#batch[@]} file(s) -> ${batch_dest}, kept for the next run" >&2
  fi
  batch=()
}

for f in "${files[@]}"; do
  d=$(dest_of "${f}")
  if [[ "${d}" != "${batch_dest}" ]] || (( ${#batch[@]} >= BATCH )); then
    ship
    batch_dest=${d}
  fi
  batch+=("${f}")
done
ship
exit "${rc}"
