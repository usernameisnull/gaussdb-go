#!/usr/bin/env bash
set -eux

export PGDATABASE=pgx_test
export PGPORT=5432
export PGUSER=gaussdb
export PGHOST=localhost
export PGX_SSL_PASSWORD=certpw
export PGX_TEST_CRATEDB_CONN_STRING='gaussdb://gaussdb:Gaussdb@123!@localhost:5432/pgx_test'
export PGX_TEST_DATABASE='host=localhost database=pgx_test user=pgx_md5 password=Gaussdb@123!'
export PGX_TEST_MD5_PASSWORD_CONN_STRING='host=localhost database=pgx_test user=pgx_md5 password=Gaussdb@123!'
export PGX_TEST_PLAIN_PASSWORD_CONN_STRING='host=localhost user=pgx_pw password=Gaussdb@123!'
export PGX_TEST_SCRAM_PASSWORD_CONN_STRING='host=localhost user=pgx_scram password=Gaussdb@123! database=pgx_test'
export PGX_TEST_TCP_CONN_STRING='host=localhost database=pgx_test user=pgx_md5 password=Gaussdb@123!'
export IS_OPENGAUSS=true
#export PGX_TEST_TLS_CLIENT_CONN_STRING=host='localhost user=pgx_sslcert sslmode=verify-full sslrootcert=`pwd`/.testdb/ca.pem database=pgx_test sslcert=`pwd`/.testdb/pgx_sslcert.crt sslkey=`pwd`/.testdb/pgx_sslcert.key'
#export PGX_TEST_TLS_CONN_STRING=host='localhost user=pgx_ssl password=Gaussdb@123! sslmode=verify-full sslrootcert=`pwd`/.testdb/ca.pem'
#export PGX_TEST_UNIX_SOCKET_CONN_STRING='host=/home/omm/tmp database=pgx_test'
export POSTGRESQL_DATA_DIR=postgresql

# parallel testing is disabled because somehow parallel testing causes Github Actions to kill the runner.
go test -parallel=1 -race ./...