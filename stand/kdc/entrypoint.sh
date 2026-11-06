#!/usr/bin/env bash
# Create the realm and every principal of the stand, publish keytabs on the shared volume.
# Hadoop containers block until /keytabs/.ready appears.
set -euo pipefail

REALM=${REALM:-TRAFFIC.LOCAL}
KEYTABS=/keytabs
rm -f "${KEYTABS}/.ready"

if [[ ! -f /var/lib/krb5kdc/principal ]]; then
  echo "[kdc] creating realm ${REALM}"
  kdb5_util create -s -r "${REALM}" -P traffic-master
fi

# principal:keytab. Service principals use the name a client derives for the server: Java
# canonicalizes the host through reverse DNS, which Docker answers with <name>.<network>.
principals=(
  "hdfs/tr-nn.traffic-net@${REALM}:tr-nn.keytab"
  "HTTP/tr-nn.traffic-net@${REALM}:tr-nn.keytab"
  "hdfs/tr-dn1.traffic-net@${REALM}:tr-dn1.keytab"
  "hdfs/tr-dn2.traffic-net@${REALM}:tr-dn2.keytab"
  "hdfs/tr-dn3.traffic-net@${REALM}:tr-dn3.keytab"
  "alice@${REALM}:alice.keytab"
  "bob@${REALM}:bob.keytab"
  "carol@${REALM}:carol.keytab"
  "dave@${REALM}:dave.keytab"
)

for keytab in $(printf '%s\n' "${principals[@]}" | cut -d: -f2 | sort -u); do
  rm -f "${KEYTABS}/${keytab}"
done

for entry in "${principals[@]}"; do
  principal="${entry%%:*}"
  keytab="${entry##*:}"
  kadmin.local -q "addprinc -randkey ${principal}" 2>&1 | grep -v "already exists" || true
  kadmin.local -q "ktadd -k ${KEYTABS}/${keytab} ${principal}" >/dev/null
  chmod 0644 "${KEYTABS}/${keytab}"
  echo "[kdc] ${KEYTABS}/${keytab} <- ${principal}"
done

cp /etc/krb5.conf "${KEYTABS}/krb5.conf"
touch "${KEYTABS}/.ready"
echo "[kdc] realm ready"
exec krb5kdc -n
