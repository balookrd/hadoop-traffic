# One image for every Hadoop node of the stand (NameNode, DataNodes, client). The distribution
# comes from the build context "dist" (compose: additional_contexts), staged by prepare.sh:
# HDP 3.1.0.0-78 or Apache 3.3.6. Both run on Java 8, like the production clusters.
FROM eclipse-temurin:8-jdk-jammy

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
    && apt-get install -y --no-install-recommends krb5-user curl procps netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Kerberos short names must exist as OS users: the NameNode resolves their groups locally.
RUN groupadd -r hadoop \
    && useradd -m -g hadoop hdfs \
    && for u in alice bob carol dave; do useradd -m "$u"; done

ENV HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/etc/hadoop/conf
ENV PATH=${HADOOP_HOME}/bin:${PATH}

COPY --from=dist . ${HADOOP_HOME}
COPY entrypoint.sh /usr/local/bin/hadoop-entrypoint
RUN chmod +x /usr/local/bin/hadoop-entrypoint ${HADOOP_HOME}/bin/* \
    && mkdir -p /data /var/log/hadoop && chown hdfs:hadoop /data /var/log/hadoop

ENTRYPOINT ["/usr/local/bin/hadoop-entrypoint"]
