#!/usr/bin/env bash
# Ship closed hdfs-traffic-agent event files from this DataNode into HDFS, then delete them.
# Run from cron every 5 minutes as the user that owns the events directory, e.g.:
#   */5 * * * * hdfs /opt/hdfs-traffic-agent/upload-events.sh -d /var/log/hadoop/hdfs-traffic \
#       -r /data/hdfs-traffic/raw -k /etc/security/keytabs/hdfs.headless.keytab -p hdfs@REALM
# Closed files are immutable and carry the UTC date of their events in the name, so a re-run
# after a failure simply puts the same file again (-f). One failing file does not stop the run:
# the rest are still shipped and the exit code is 1.
# The partition dn=<host> is the node key (default: hostname -f, override with -H); it may
# differ from the "dn" field inside the events.
set -euo pipefail

EVENTS_DIR=/var/log/hadoop/hdfs-traffic
ROOT=/data/hdfs-traffic/raw
PREFIX=events
KEYTAB=
PRINCIPAL=
HOST=

usage() {
  echo "usage: $0 [-d events_dir] [-r hdfs_root] [-n file_prefix] [-k keytab -p principal] [-H host]" >&2
  exit 2
}

while getopts "d:r:n:k:p:H:" opt; do
  case "${opt}" in
    d) EVENTS_DIR=${OPTARG} ;;
    r) ROOT=${OPTARG} ;;
    n) PREFIX=${OPTARG} ;;
    k) KEYTAB=${OPTARG} ;;
    p) PRINCIPAL=${OPTARG} ;;
    H) HOST=${OPTARG} ;;
    *) usage ;;
  esac
done
if [[ -n "${KEYTAB}" && -z "${PRINCIPAL}" ]]; then
  usage
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

if [[ -n "${KEYTAB}" ]]; then
  export KRB5CCNAME="FILE:${EVENTS_DIR}/.upload.krb5cc"
  kinit -kt "${KEYTAB}" "${PRINCIPAL}"
fi

host=${HOST:-$(hostname -f)}
created=
rc=0
shopt -s nullglob
for f in "${EVENTS_DIR}/${PREFIX}"-[0-9]*-[0-9]*-[0-9]*.jsonl; do
  name=$(basename "${f}")
  stamp=${name#"${PREFIX}"-}
  day=${stamp:0:8}
  dest="${ROOT}/dt=${day:0:4}-${day:4:2}-${day:6:2}/dn=${host}"
  if { [[ "${created}" == *"|${dest}|"* ]] || hdfs dfs -mkdir -p "${dest}"; } \
      && hdfs dfs -put -f "${f}" "${dest}/${name}" \
      && rm -f "${f}"; then
    created="${created}|${dest}|"
    echo "upload-events: ${name} -> ${dest}"
  else
    rc=1
    echo "upload-events: failed ${name}" >&2
  fi
done
exit "${rc}"
