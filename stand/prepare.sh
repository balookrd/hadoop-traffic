#!/usr/bin/env bash
# Stage a Hadoop distribution for the stand into dist/<flavor>.
#   ./prepare.sh hdp      # HDP 3.1.0.0-78 from HDP_DIST_DIR (default ~/hdp/3.1.0.0-78)
#   ./prepare.sh apache   # Apache Hadoop 3.3.6 from the apache/hadoop:3.3.6 image
# HDP jars are not redistributable and are not in any public repository, hence a local copy.
set -euo pipefail

STAND_DIR=$(cd "$(dirname "$0")" && pwd)
FLAVOR=${1:-hdp}
DEST="${STAND_DIR}/dist/${FLAVOR}"

if [[ -d "${DEST}/share/hadoop/hdfs" && "${RESTAGE:-false}" != "true" ]]; then
  echo "[prepare] ${FLAVOR} already staged in ${DEST} (RESTAGE=true to redo)"
else
  rm -rf "${DEST}"
  mkdir -p "${DEST}"
  case "${FLAVOR}" in
    hdp)
      HDP_DIST_DIR=${HDP_DIST_DIR:-${HOME}/hdp/3.1.0.0-78}
      tarball="${HDP_DIST_DIR}/hadoop/mapreduce.tar.gz"
      [[ -f "${tarball}" ]] || { echo "[prepare] ${tarball} not found; set HDP_DIST_DIR" >&2; exit 1; }
      # mapreduce.tar.gz is a self-contained Hadoop: common, hdfs, bin, libexec, etc.
      echo "[prepare] unpacking ${tarball}"
      tar xzf "${tarball}" -C "${DEST}" --strip-components=1 \
        --exclude='*/sources/*' --exclude='*-tests.jar' --exclude='*/share/doc/*'
      ;;
    apache)
      image=apache/hadoop:3.3.6
      echo "[prepare] copying /opt/hadoop out of ${image}"
      docker pull --platform linux/amd64 "${image}"
      cid=$(docker create --platform linux/amd64 "${image}")
      trap 'docker rm -f "${cid}" >/dev/null' EXIT
      docker cp "${cid}:/opt/hadoop/." "${DEST}"
      rm -rf "${DEST}/share/doc"
      ;;
    *)
      echo "usage: $0 hdp|apache" >&2
      exit 1
      ;;
  esac
  echo "[prepare] staged $(du -sh "${DEST}" | cut -f1) into ${DEST}"
fi

jar="${STAND_DIR}/../agent/target/hdfs-traffic-agent-0.1.0-SNAPSHOT.jar"
[[ -f "${jar}" ]] || echo "[prepare] WARNING: ${jar} is missing, build it with mvn package (JDK 8)" >&2
