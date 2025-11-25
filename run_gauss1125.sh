#!/usr/bin/env bash
set -euo pipefail
source clean_database.sh

whoami
timestamp=$(date +"%Y%m%d%H%M%S")
logFile="mine/logs/gaussdb-${timestamp}.log"

export GAUSSDB_TEST_CRATEDB_CONN_STRING="gaussdb://${USER}:${PASSWORD}@${HOST}:${PORT}/${TARGET_DB}"
export GAUSSDB_TEST_DATABASE="host=${HOST} port=${PORT} user=${USER} password=${PASSWORD} database=${TARGET_DB}"
export GAUSSDB_TEST_MD5_PASSWORD_CONN_STRING="host=${HOST} port=${PORT} user=${USER} password=${PASSWORD} database=${TARGET_DB}"
export GAUSSDB_TEST_PLAIN_PASSWORD_CONN_STRING="host=${HOST} port=${PORT} user=${USER} password=${PASSWORD} database=${TARGET_DB}"
export GAUSSDB_TEST_SCRAM_PASSWORD_CONN_STRING="host=${HOST} port=${PORT} user=${USER} password=${PASSWORD} database=${TARGET_DB}"
export GAUSSDB_TEST_TCP_CONN_STRING="host=${HOST} port=${PORT} user=${USER} password=${PASSWORD} database=${TARGET_DB}"

# export GAUSSDB_TEST_TLS_CLIENT_CONN_STRING="host='${HOST}' user=gaussdbgo_sslcert sslmode=verify-full sslrootcert=${CURRENT_DIR}/.testdb/ca.pem database=${TARGET_DB} sslcert=${CURRENT_DIR}/.testdb/gaussdbgo_sslcert.crt sslkey=${CURRENT_DIR}/.testdb/gaussdbgo_sslcert.key"
# export GAUSSDB_TEST_TLS_CONN_STRING="host='${HOST}' user=gaussdbgo_ssl password=${PASSWORD} sslmode=verify-full sslrootcert=${CURRENT_DIR}/.testdb/ca.pem"
# export GAUSSDB_TEST_UNIX_SOCKET_CONN_STRING="host=/home/omm/tmp database=${TARGET_DB}"
# export POSTGRESQL_DATA_DIR="postgresql"

# -shuffle=off 保证每次的执行顺序是一样的
go test -shuffle=off -count=1 ./... | tee -a "${logFile}"
