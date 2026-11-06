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

SPARK_VERSION=3.5.1
SPARK_NAME="spark-${SPARK_VERSION}-bin-hadoop3"
SPARK_DIR="${STAND_DIR}/dist/spark/${SPARK_NAME}"
if [[ ! -x "${SPARK_DIR}/bin/spark-submit" ]]; then
  mkdir -p "${STAND_DIR}/dist/spark"
  url="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_NAME}.tgz"
  echo "[prepare] downloading ${url}"
  curl -fL --retry 3 -o "${STAND_DIR}/dist/spark/${SPARK_NAME}.tgz" "${url}"
  expected=$(curl -fsL "${url}.sha512" | tr -d ' \n\r' | grep -oiE '[0-9a-f]{128}' | head -1 \
    | tr 'A-F' 'a-f')
  actual=$(shasum -a 512 "${STAND_DIR}/dist/spark/${SPARK_NAME}.tgz" | cut -d' ' -f1)
  [[ -n "${expected}" && "${expected}" == "${actual}" ]] \
    || { echo "[prepare] sha512 mismatch for ${SPARK_NAME}.tgz" >&2; exit 1; }
  tar xzf "${STAND_DIR}/dist/spark/${SPARK_NAME}.tgz" -C "${STAND_DIR}/dist/spark"
  rm "${STAND_DIR}/dist/spark/${SPARK_NAME}.tgz"
fi
echo "[prepare] Spark ${SPARK_VERSION} staged in ${SPARK_DIR}"

jar="${STAND_DIR}/../agent/target/hdfs-traffic-agent-0.1.0-SNAPSHOT.jar"
[[ -f "${jar}" ]] || echo "[prepare] WARNING: ${jar} is missing, build it with mvn package (JDK 8)" >&2
