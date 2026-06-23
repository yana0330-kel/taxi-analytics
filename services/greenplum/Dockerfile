FROM woblerr/greenplum:6.27.1

USER root

# JDBC drivers for PXF JDBC profile:
# - PostgreSQL: raw Postgres -> Greenplum
# - ClickHouse: Greenplum -> ClickHouse
#   Uses legacy shaded ru.yandex driver to avoid PXF JVM error:
#   Could not initialize class com.clickhouse.jdbc.ClickHouseDriver
#
# Important:
# PXF does not reliably load custom JDBC drivers from /usr/local/pxf/lib in this image.
# We download jars into a neutral directory and then copy them into $PXF_BASE/lib
# from the init script, because $PXF_BASE usually lives under the persistent /data volume.
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates curl; \
    mkdir -p /opt/fpp-jdbc-drivers; \
    curl -fsSL -o /opt/fpp-jdbc-drivers/postgresql-42.7.4.jar \
      https://jdbc.postgresql.org/download/postgresql-42.7.4.jar; \
    curl -fsSL -o /opt/fpp-jdbc-drivers/clickhouse-jdbc-0.3.2-shaded.jar \
      https://repo1.maven.org/maven2/ru/yandex/clickhouse/clickhouse-jdbc/0.3.2/clickhouse-jdbc-0.3.2-shaded.jar; \
    chmod 0644 /opt/fpp-jdbc-drivers/*.jar; \
    chown -R gpadmin:gpadmin /opt/fpp-jdbc-drivers; \
    apt-get clean; \
    rm -rf /var/lib/apt/lists/*

COPY initdb.d/ /docker-entrypoint-initdb.d/
RUN set -eux; \
    chmod +x /docker-entrypoint-initdb.d/*.sh; \
    chown -R gpadmin:gpadmin /docker-entrypoint-initdb.d

USER root
