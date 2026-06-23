#!/usr/bin/env bash
set -Eeuo pipefail

source "/home/${GREENPLUM_USER:-gpadmin}/.bashrc"

: "${PXF_BASE:=/data/pxf}"
: "${RAW_DB:=rawdb}"
: "${RAW_USER:=raw_user}"
: "${RAW_PASSWORD:=raw_pass}"
: "${CLICKHOUSE_DB:=dm_ch}"
: "${CLICKHOUSE_USER:=fpp_user}"
: "${CLICKHOUSE_PASSWORD:=fpp_pass}"
: "${PXF_JVM_OPTS_FPP:=-Xms128m -Xmx384m}"

mkdir -p \
  "${PXF_BASE}/servers/raw_pg" \
  "${PXF_BASE}/servers/clickhouse" \
  "${PXF_BASE}/lib"

# Register JDBC drivers in PXF_BASE/lib.
# This directory is included in PXF runtime classpath after pxf cluster sync/restart.
# Keep exactly one ClickHouse JDBC driver in the PXF classpath.
# The legacy shaded ru.yandex driver is used intentionally because the newer
# com.clickhouse driver may fail in PXF with:
# "Could not initialize class com.clickhouse.jdbc.ClickHouseDriver".
rm -f "${PXF_BASE}/lib/"*clickhouse*.jar || true
cp -f /opt/fpp-jdbc-drivers/postgresql-42.7.4.jar "${PXF_BASE}/lib/"
cp -f /opt/fpp-jdbc-drivers/clickhouse-jdbc-0.3.2-shaded.jar "${PXF_BASE}/lib/"
chown -R "${GREENPLUM_USER:-gpadmin}:${GREENPLUM_USER:-gpadmin}" "${PXF_BASE}/lib" || true
chmod 0644 "${PXF_BASE}/lib"/*.jar || true

echo "INFO - PXF JDBC drivers in ${PXF_BASE}/lib:"
ls -lh "${PXF_BASE}/lib" || true

cat > "${PXF_BASE}/servers/raw_pg/jdbc-site.xml" <<XML
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>jdbc.driver</name>
    <value>org.postgresql.Driver</value>
  </property>
  <property>
    <name>jdbc.url</name>
    <value>jdbc:postgresql://postgres:5432/${RAW_DB}</value>
  </property>
  <property>
    <name>jdbc.user</name>
    <value>${RAW_USER}</value>
  </property>
  <property>
    <name>jdbc.password</name>
    <value>${RAW_PASSWORD}</value>
  </property>
  <property>
    <name>jdbc.statement.queryTimeout</name>
    <value>120</value>
  </property>
</configuration>
XML

cat > "${PXF_BASE}/servers/clickhouse/jdbc-site.xml" <<XML
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>jdbc.driver</name>
    <value>ru.yandex.clickhouse.ClickHouseDriver</value>
  </property>
  <property>
    <name>jdbc.url</name>
    <value>jdbc:clickhouse://clickhouse:8123/${CLICKHOUSE_DB}</value>
  </property>
  <property>
    <name>jdbc.user</name>
    <value>${CLICKHOUSE_USER}</value>
  </property>
  <property>
    <name>jdbc.password</name>
    <value>${CLICKHOUSE_PASSWORD}</value>
  </property>
  <property>
    <name>jdbc.statement.writeSize</name>
    <value>1000</value>
  </property>
  <property>
    <name>jdbc.statement.queryTimeout</name>
    <value>120</value>
  </property>
</configuration>
XML

# Lower PXF JVM memory for local Docker Desktop usage.
PXF_ENV="${PXF_BASE}/conf/pxf-env.sh"
if [ -f "${PXF_ENV}" ]; then
  sed -i '/^PXF_JVM_OPTS=/d' "${PXF_ENV}" || true
  echo "PXF_JVM_OPTS=\"${PXF_JVM_OPTS_FPP}\"" >> "${PXF_ENV}"
fi

# PXF has already been prepared/registered by the base image before custom init scripts.
# Sync and restart it after adding JDBC driver jars and JDBC server configs.
if command -v pxf >/dev/null 2>&1; then
  echo "INFO - Sync PXF config after adding raw_pg/clickhouse JDBC servers and driver jars"
  pxf cluster sync || true
  echo "INFO - Restart PXF cluster after config sync"
  pxf cluster restart || { pxf cluster stop || true; pxf cluster start || true; }
  pxf cluster status || true
fi
