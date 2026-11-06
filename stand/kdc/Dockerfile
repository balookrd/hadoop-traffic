# MIT KDC for the stand: realm TRAFFIC.LOCAL, keytabs published on a shared volume.
FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
    && apt-get install -y --no-install-recommends krb5-kdc krb5-admin-server krb5-user \
    && rm -rf /var/lib/apt/lists/*

COPY krb5.conf /etc/krb5.conf
COPY entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/entrypoint.sh

EXPOSE 88
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
