#!/usr/bin/env bash
# Daily report for a UTC day (default: yesterday): NameNode checkpoint -> block-mapper ->
# CompactEvents -> ResolvePaths.
#
# Requires GNU date, flock, python3 (JMX JSON parsing), curl with SPNEGO (--negotiate).
#
# `hdfs dfsadmin -fetchImage` returns the LAST NAMENODE CHECKPOINT, not the live namespace, and
# this script never calls saveNamespace (it would block the active NameNode in safe mode).
# So before fetching it waits until the active NameNode reports a checkpoint taken after
# 00:00 UTC of today. That needs a working checkpointer: the standby NameNode (HA) or the
# SecondaryNameNode, with dfs.namenode.checkpoint.period <= 1 hour.
#
# Users:
#  - fetchImage and the mapper run as the invoking user, who must be an HDFS cluster
#    administrator (member of dfs.cluster.administrators, e.g. hdfs): -k/-p log it in
#    (both or neither).
#  - Spark jobs must NOT run as hdfs on a secure cluster (LinuxContainerExecutor bans it):
#    -s/-t pass a Spark principal and keytab to spark-submit (--principal/--keytab). That user
#    needs write access to <root>/{events,event_path,traffic_minute} and read access to
#    <root>/{raw,fsimage}. -s/-t are required with `-c hive` or `--master yarn`; they may be
#    omitted for local tests.
#
# -n is the HTTP address of the ACTIVE NameNode (HA: find it with
# `hdfs haadmin -getAllServiceState`), e.g. http://nn1.example:50070 on HDP.
#
# Reruns: -D YYYY-MM-DD recompacts and re-resolves a given day against the latest image
# (late raw files, e.g. from a DataNode that was down at midnight, are picked up; both jobs
# are idempotent per partition). To replace a stale fsimage/dt=<today>, delete it first:
#   hdfs dfs -rm -r -skipTrash <root>/fsimage/dt=<today>
#
# Cron (02:00 UTC, i.e. after a post-midnight checkpoint). Cron needs one physical line per
# entry; without CRON_TZ=UTC the host timezone applies and 02:00 is host-local time:
#   CRON_TZ=UTC
#   0 2 * * * hdfs /opt/hdfs-traffic/daily-report.sh -n http://nn1.example:50070 -k /etc/security/keytabs/hdfs.headless.keytab -p hdfs-cluster@REALM -s spark-report@REALM -t /etc/security/keytabs/spark-report.keytab >>/var/log/hdfs-traffic-report.log 2>&1
#
# -N name=cidr[,name=cidr...] names client networks for traffic_minute.origin, e.g.
#   -N own=10.0.0.0/24,cluster_b=172.16.0.0/24
# Local clients get origin 'local', remote peers outside these networks get 'other'.
#
# The Spark keytab (-t) must be readable by the user running cron. For an HTTPS-only NameNode
# pass -n https://<nn>:50470.
#
# Raw retention: -K <days> removes <root>/raw/dt=<d> for every d more than <days> days before
# the report day, after both Spark jobs succeeded. A partition goes only if
# <root>/events/dt=<d>/_SUCCESS exists (CompactEvents finished for that day); others are kept
# and reported. Without -K nothing is ever removed. With -K, -D refuses a day whose raw
# partition is already past retention: CompactEvents would overwrite events/dt=<day> with
# whatever late files a DataNode shipped after the removal.
set -euo pipefail

ROOT=/data/hdfs-traffic
WORK=/var/tmp/hdfs-traffic
MAPPER_JAR=/opt/hdfs-traffic/block-mapper.jar
JOBS_JAR=/opt/hdfs-traffic/spark-jobs.jar
SPARK_SUBMIT=${SPARK_HOME:-/opt/spark}/bin/spark-submit
SPARK_ARGS=${SPARK_ARGS:---master yarn --deploy-mode client}
POLL_SEC=${POLL_SEC:-300}
MAX_WAIT_SEC=${MAX_WAIT_SEC:-10800}
CATALOG=hive
KEYTAB=
PRINCIPAL=
SPARK_PRINCIPAL=
SPARK_KEYTAB=
NN_HTTP=
DAY=
NETS=
RAW_KEEP_DAYS=

usage() {
  echo "usage: $0 -n <active NameNode http URL> [-D YYYY-MM-DD] [-r root] [-w workdir]" \
    "[-m mapper.jar] [-j spark-jobs.jar] [-c hive|none] [-k hdfs keytab -p hdfs principal]" \
    "[-s spark principal -t spark keytab] [-N name=cidr[,name=cidr...]] [-K raw_keep_days]" >&2
  exit 2
}
die() { echo "daily-report: $*" >&2; exit 1; }

while getopts "r:w:m:j:c:k:p:n:D:s:t:N:K:" opt; do
  case "${opt}" in
    r) ROOT=${OPTARG} ;; w) WORK=${OPTARG} ;; m) MAPPER_JAR=${OPTARG} ;;
    j) JOBS_JAR=${OPTARG} ;; c) CATALOG=${OPTARG} ;; k) KEYTAB=${OPTARG} ;;
    p) PRINCIPAL=${OPTARG} ;; n) NN_HTTP=${OPTARG} ;; D) DAY=${OPTARG} ;;
    s) SPARK_PRINCIPAL=${OPTARG} ;; t) SPARK_KEYTAB=${OPTARG} ;; N) NETS=${OPTARG} ;;
    K) RAW_KEEP_DAYS=${OPTARG} ;;
    *) usage ;;
  esac
done
if [[ -n "${RAW_KEEP_DAYS}" ]] && ! [[ "${RAW_KEEP_DAYS}" =~ ^[0-9]{1,4}$ ]]; then
  die "bad -K (expected a number of days): ${RAW_KEEP_DAYS}"
fi
[[ -n "${NN_HTTP}" ]] || usage
NN_HTTP=${NN_HTTP%/}
[[ "${CATALOG}" == hive || "${CATALOG}" == none ]] || usage
[[ "${POLL_SEC}" =~ ^[0-9]+$ && "${POLL_SEC}" -gt 0 ]] || die "bad POLL_SEC: ${POLL_SEC}"
[[ "${MAX_WAIT_SEC}" =~ ^[0-9]+$ ]] || die "bad MAX_WAIT_SEC: ${MAX_WAIT_SEC}"
if [[ -n "${DAY}" ]] && ! [[ "${DAY}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  die "bad -D (expected YYYY-MM-DD): ${DAY}"
fi
if [[ -n "${DAY}" ]]; then
  date -u -d "${DAY}" +%F >/dev/null 2>&1 || die "bad -D (not a date): ${DAY}"
fi
if [[ -n "${KEYTAB}" && -z "${PRINCIPAL}" ]] || [[ -z "${KEYTAB}" && -n "${PRINCIPAL}" ]]; then
  die "-k and -p go together"
fi
if [[ -n "${SPARK_KEYTAB}" && -z "${SPARK_PRINCIPAL}" ]] \
    || [[ -z "${SPARK_KEYTAB}" && -n "${SPARK_PRINCIPAL}" ]]; then
  die "-s and -t go together"
fi
if [[ -z "${SPARK_PRINCIPAL}" && ( "${CATALOG}" == hive || "${SPARK_ARGS}" == *yarn* ) ]]; then
  die "-s <spark principal> -t <spark keytab> are required with -c hive or --master yarn"
fi
command -v python3 >/dev/null 2>&1 || die "python3 is required to parse NameNode JMX"

if [[ -n "${DAY}" && -n "${RAW_KEEP_DAYS}" ]]; then
  # The daily run of today keeps raw partitions from (yesterday - K) on.
  oldest_kept=$(date -u -d "$(date -u +%F) $(( RAW_KEEP_DAYS + 1 )) days ago" +%F)
  if [[ "${DAY}" < "${oldest_kept}" ]]; then
    die "-D ${DAY} is before ${oldest_kept}, the oldest raw partition kept by -K" \
      "${RAW_KEEP_DAYS}: a rerun would overwrite events/dt=${DAY} with incomplete data." \
      "If ${ROOT}/raw/dt=${DAY} is still complete, rerun without -K"
  fi
fi

mkdir -p "${WORK}"
exec 9>"${WORK}/.lock"
if ! flock -n 9; then
  echo "daily-report: another run holds ${WORK}/.lock; exiting"
  exit 0
fi

img="${WORK}/fsimg"
trap 'rm -rf "${img}"' EXIT

if [[ -n "${KEYTAB}" ]]; then
  export KRB5CCNAME="FILE:${WORK}/krb5cc"
  kinit -kt "${KEYTAB}" "${PRINCIPAL}"
fi

today=$(date -u +%F)
if [[ -n "${DAY}" ]]; then
  dt=${DAY}
  image_dt_args=()
else
  dt=$(date -u -d yesterday +%F)
  image_dt_args=(--image-dt "${today}")
fi

nets_args=()
if [[ -n "${NETS}" ]]; then
  nets_args=(--nets "${NETS}")
fi

spark_auth=()
if [[ -n "${SPARK_PRINCIPAL}" ]]; then
  spark_auth=(--principal "${SPARK_PRINCIPAL}" --keytab "${SPARK_KEYTAB}")
fi

# Last checkpoint of the active NameNode, epoch ms, in CP_MS (empty when unavailable).
# The first failure prints curl's error once; HTTP 401/403 is fatal (will not fix itself).
CP_MS=
cp_err_shown=0
checkpoint_ms() {
  local errf out rc=0 code body
  errf=$(mktemp "${WORK}/curl-err.XXXXXX")
  out=$(curl -sS --negotiate -u : -w $'\n%{http_code}' \
      "${NN_HTTP}/jmx?qry=Hadoop:service=NameNode,name=FSNamesystem" 2>"${errf}") || rc=$?
  code=${out##*$'\n'}
  body=${out%$'\n'*}
  CP_MS=
  if [[ "${code}" == 401 || "${code}" == 403 ]]; then
    rm -f "${errf}"
    die "NameNode JMX at ${NN_HTTP} answered HTTP ${code}: check the Kerberos ticket (-k/-p)" \
      "and that the user may read JMX"
  fi
  if (( rc == 0 )) && [[ "${code}" == 200 ]]; then
    CP_MS=$(printf '%s' "${body}" | python3 -c \
      'import json,sys; print(int(json.load(sys.stdin)["beans"][0]["LastCheckpointTime"]))' \
      2>/dev/null) || CP_MS=
  fi
  if [[ -z "${CP_MS}" ]] && (( cp_err_shown == 0 )); then
    cp_err_shown=1
    echo "daily-report: cannot read checkpoint time from ${NN_HTTP} (curl exit ${rc}," \
      "HTTP ${code}): $(tr '\n' ' ' <"${errf}")" >&2
  fi
  rm -f "${errf}"
}

if ! hdfs dfs -test -d "${ROOT}/fsimage/dt=${today}"; then
  midnight_ms=$(( $(date -u -d "${today} 00:00:00" +%s) * 1000 ))
  waited=0
  while :; do
    checkpoint_ms
    cp_ms=${CP_MS}
    if [[ "${cp_ms}" =~ ^[0-9]+$ ]] && (( cp_ms >= midnight_ms )); then
      break
    fi
    if (( waited >= MAX_WAIT_SEC )); then
      if [[ "${cp_ms}" =~ ^[0-9]+$ && "${cp_ms}" -gt 0 ]]; then
        last=$(date -u -d "@$(( cp_ms / 1000 ))" '+%F %T UTC')
      else
        last="unknown (JMX at ${NN_HTTP} unreadable)"
      fi
      die "last checkpoint ${last} is before ${today} 00:00:00 UTC; is checkpointing" \
        "(standby NameNode / SecondaryNameNode) healthy?"
    fi
    echo "daily-report: waiting for a checkpoint after ${today} 00:00 UTC (${waited}s)"
    sleep "${POLL_SEC}"
    waited=$(( waited + POLL_SEC ))
  done

  rm -rf "${img}" && mkdir -p "${img}"
  hdfs dfsadmin -fetchImage "${img}"
  image=$(ls "${img}"/fsimage_[0-9]* | grep -v '\.md5$' | sort | tail -1 || true)
  [[ -n "${image}" ]] || die "no fsimage downloaded into ${img}"
  hadoop jar "${MAPPER_JAR}" org.hadooptraffic.mapper.FsImageMapper \
    -i "${image}" -o "${ROOT}/fsimage/dt=${today}"
fi

# shellcheck disable=SC2086
"${SPARK_SUBMIT}" ${SPARK_ARGS} ${spark_auth[@]+"${spark_auth[@]}"} --class org.hadooptraffic.spark.CompactEvents \
  "${JOBS_JAR}" --dt "${dt}" --root "${ROOT}" --catalog "${CATALOG}"
# shellcheck disable=SC2086
"${SPARK_SUBMIT}" ${SPARK_ARGS} ${spark_auth[@]+"${spark_auth[@]}"} --class org.hadooptraffic.spark.ResolvePaths \
  "${JOBS_JAR}" --dt "${dt}" ${image_dt_args[@]+"${image_dt_args[@]}"} --root "${ROOT}" --catalog "${CATALOG}" \
  ${nets_args[@]+"${nets_args[@]}"}

# Raw retention (-K): only after both jobs succeeded, only days CompactEvents finished.
if [[ -n "${RAW_KEEP_DAYS}" ]]; then
  cutoff=$(date -u -d "${dt} ${RAW_KEEP_DAYS} days ago" +%F)
  raw_dirs=$(hdfs dfs -ls -C "${ROOT}/raw")
  # No match is not an error: then nothing is compacted and nothing is removed.
  compacted=$(hdfs dfs -ls -C "${ROOT}/events/dt=*/_SUCCESS" 2>/dev/null || true)
  doomed=()
  for p in ${raw_dirs}; do
    [[ "${p##*/}" =~ ^dt=([0-9]{4}-[0-9]{2}-[0-9]{2})$ ]] || continue
    d=${BASH_REMATCH[1]}
    [[ "${d}" < "${cutoff}" ]] || continue
    if [[ $'\n'"${compacted}"$'\n' == *"/events/dt=${d}/_SUCCESS"$'\n'* ]]; then
      doomed+=("${p}")
    else
      echo "daily-report: keeping ${p}: no ${ROOT}/events/dt=${d}/_SUCCESS," \
        "the day is not compacted" >&2
    fi
  done
  if (( ${#doomed[@]} > 0 )); then
    hdfs dfs -rm -r -skipTrash "${doomed[@]}"
    echo "daily-report: removed ${#doomed[@]} raw partition(s) before ${cutoff}"
  fi
fi
