#!/usr/bin/env bash
# Configure a secure HDFS node and run it in the foreground.
#   ROLE=namenode | datanode | client
# Security mirrors production: Kerberos, block access tokens, SASL on the data transfer
# protocol. ignore.secure.ports.for.testing lets a secure DataNode keep unprivileged ports
# without jsvc or HTTPS, which is all a container needs.
set -euo pipefail

ROLE=${ROLE:?ROLE must be namenode, datanode or client}
REALM=${REALM:-TRAFFIC.LOCAL}
HOST=$(hostname -s)
# Service principal host part: what clients get from reverse DNS (<name>.<network>).
FQDN="${HOST}.${DOMAIN:-traffic-net}"
# A DataNode logs in with its own literal principal. Everywhere else the key is a template the
# NameNode resolves per connecting DataNode (service-level authorization of DatanodeProtocol).
if [[ "${ROLE}" == "datanode" ]]; then
  DN_PRINCIPAL="hdfs/${FQDN}@${REALM}"
else
  DN_PRINCIPAL="hdfs/_HOST@${REALM}"
fi
CONF=${HADOOP_CONF_DIR}
KEYTAB=/etc/security/keytabs/host.keytab

echo "[${HOST}] waiting for keytabs"
until [[ -f /keytabs/.ready ]]; do sleep 1; done
cp /keytabs/krb5.conf /etc/krb5.conf

mkdir -p "${CONF}" /etc/security/keytabs
# Our own log4j config: distributions differ (Apache's image logs to stdout, which would mix
# with `hdfs dfs -cat` output). Console on stderr; docker logs shows the daemons.
cat > "${CONF}/log4j.properties" <<'LOG4J'
hadoop.root.logger=INFO,console
log4j.rootLogger=${hadoop.root.logger}
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.target=System.err
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{ISO8601} %p %c{2}: %m%n
log4j.appender.NullAppender=org.apache.log4j.varia.NullAppender
LOG4J
if [[ -f "/keytabs/${HOST}.keytab" ]]; then
  install -o hdfs -g hadoop -m 0400 "/keytabs/${HOST}.keytab" "${KEYTAB}"
fi

# SPNEGO on the NameNode web endpoints. Hadoop 3.3 builds the WebHDFS AuthFilter from
# hadoop.http.authentication.* (3.1 reads dfs.web.authentication.*), so set both. DataNodes
# keep simple HTTP auth: their /jmx stays readable without a ticket.
NN_HTTP_AUTH=""
if [[ "${ROLE}" == "namenode" ]]; then
  NN_HTTP_AUTH="  <property><name>hadoop.http.authentication.type</name><value>kerberos</value></property>
  <property><name>hadoop.http.authentication.kerberos.principal</name><value>*</value></property>
  <property><name>hadoop.http.authentication.kerberos.keytab</name><value>${KEYTAB}</value></property>
  <property><name>hadoop.http.authentication.simple.anonymous.allowed</name><value>false</value></property>"
fi

cat > "${CONF}/core-site.xml" <<XML
<?xml version="1.0"?>
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://tr-nn:8020</value></property>
  <property><name>hadoop.security.authentication</name><value>kerberos</value></property>
  <property><name>hadoop.security.authorization</name><value>true</value></property>
  <property><name>hadoop.rpc.protection</name><value>authentication</value></property>
${NN_HTTP_AUTH}
  <property>
    <name>hadoop.security.auth_to_local</name>
    <value>RULE:[2:\$1@\$0](.*@${REALM})s/@.*//
RULE:[1:\$1@\$0](.*@${REALM})s/@.*//
DEFAULT</value>
  </property>
</configuration>
XML

cat > "${CONF}/hdfs-site.xml" <<XML
<?xml version="1.0"?>
<configuration>
  <property><name>dfs.namenode.rpc-address</name><value>tr-nn:8020</value></property>
  <property><name>dfs.namenode.http-address</name><value>0.0.0.0:9870</value></property>
  <property><name>dfs.namenode.name.dir</name><value>file:///data/nn</value></property>
  <property><name>dfs.datanode.data.dir</name><value>file:///data/dn</value></property>
  <property><name>dfs.replication</name><value>3</value></property>
  <property><name>dfs.permissions.superusergroup</name><value>hadoop</value></property>
  <property><name>dfs.webhdfs.enabled</name><value>true</value></property>
  <property><name>dfs.namenode.datanode.registration.ip-hostname-check</name><value>false</value></property>
  <property><name>dfs.client.use.datanode.hostname</name><value>true</value></property>
  <property><name>dfs.namenode.redundancy.interval.seconds</name><value>1</value></property>

  <property><name>dfs.block.access.token.enable</name><value>true</value></property>
  <property><name>dfs.data.transfer.protection</name><value>authentication</value></property>
  <property><name>ignore.secure.ports.for.testing</name><value>true</value></property>

  <property><name>dfs.namenode.kerberos.principal</name><value>hdfs/tr-nn.${DOMAIN:-traffic-net}@${REALM}</value></property>
  <property><name>dfs.namenode.kerberos.principal.pattern</name><value>*</value></property>
  <property><name>dfs.namenode.keytab.file</name><value>${KEYTAB}</value></property>
  <property><name>dfs.namenode.kerberos.internal.spnego.principal</name><value>HTTP/tr-nn.${DOMAIN:-traffic-net}@${REALM}</value></property>
  <property><name>dfs.web.authentication.kerberos.principal</name><value>*</value></property>
  <property><name>dfs.web.authentication.kerberos.keytab</name><value>${KEYTAB}</value></property>

  <property><name>dfs.datanode.kerberos.principal</name><value>${DN_PRINCIPAL}</value></property>
  <property><name>dfs.datanode.kerberos.principal.pattern</name><value>*</value></property>
  <property><name>dfs.datanode.keytab.file</name><value>${KEYTAB}</value></property>
  <property><name>dfs.datanode.address</name><value>0.0.0.0:9866</value></property>
  <property><name>dfs.datanode.http.address</name><value>0.0.0.0:9864</value></property>
  <property><name>dfs.datanode.ipc.address</name><value>0.0.0.0:9867</value></property>
</configuration>
XML

cat > "${CONF}/hadoop-env.sh" <<ENV
export JAVA_HOME=${JAVA_HOME}
export HADOOP_LOG_DIR=/var/log/hadoop
export HADOOP_HEAPSIZE_MAX=512m
ENV

if [[ "${ROLE}" == "datanode" && -n "${AGENT_JAR:-}" ]]; then
  if [[ -f "${AGENT_JAR}" ]]; then
    echo "export HDFS_DATANODE_OPTS=\"-javaagent:${AGENT_JAR}=${AGENT_ARGS:-maxUsers=2000} \${HDFS_DATANODE_OPTS:-}\"" \
      >> "${CONF}/hadoop-env.sh"
    echo "[${HOST}] traffic agent: ${AGENT_JAR}=${AGENT_ARGS:-maxUsers=2000}"
  else
    echo "[${HOST}] WARNING: ${AGENT_JAR} not found, DataNode starts without the agent" >&2
  fi
fi

chown -R hdfs:hadoop "${CONF}"
hdfs_as() { exec runuser -u hdfs -- "${HADOOP_HOME}/bin/hdfs" "$@"; }

case "${ROLE}" in
  namenode)
    if [[ ! -d /data/nn/current ]]; then
      runuser -u hdfs -- "${HADOOP_HOME}/bin/hdfs" namenode -format -force -nonInteractive
    fi
    hdfs_as namenode
    ;;
  datanode)
    hdfs_as datanode
    ;;
  client)
    exec sleep infinity
    ;;
  *)
    echo "unknown ROLE ${ROLE}" >&2
    exit 1
    ;;
esac
