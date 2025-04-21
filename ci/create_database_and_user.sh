#!/usr/bin/env bash
set -eux

max_retries=30
retry_count=0
interval_check_time=10
container_name="${CONTAINER_NAME}"
password="${OPENGAUSS_PASSWORD}"
port=${PORT}


if [ -t 0 ]; then
  TTY_FLAG="-t"
else
  TTY_FLAG=""
fi

# verify can connect database by username and password, and show database version.
function show_database_version() {
  set +e
  docker exec -i ${TTY_FLAG} "${container_name}" bash -c "su - omm -c 'gsql -U gaussdb -p ${port} -W${password} -d postgres -c \"select version(); show server_version;\"'"
  set -e
}

sleep ${interval_check_time}
docker ps -a

while [ $retry_count -lt $max_retries ]; do
    # if not running, quit
    if [ "$(docker inspect -f '{{.State.Status}}' "${container_name}")" != "running" ]; then
        echo "Container '${container_name}' is not running."
        break
    fi

    set +e
    output=$(docker exec -i "${container_name}" bash -c "su - omm -c 'gsql -U omm -c \"select 1;\"'" 2>&1)
    status=$?
    set -e
    # when debug wo need to see what happened
    echo "${output}"

    if [ $status -eq 0 ]; then
      docker exec -i ${TTY_FLAG} "${container_name}" bash -c "su - omm -c 'gsql -U omm -c \"CREATE DATABASE pgx_test DBCOMPATIBILITY 'pg';\" -f /tmp/opengauss_setup.sql'"
      echo "Database initialization completed."
      show_database_version
      exit 0
    fi

    echo "Waiting for database to be ready... (attempt $((retry_count + 1))/$max_retries)"
    sleep ${interval_check_time}
    ((++retry_count))
done

echo "Database initialization failed."
docker logs "${container_name}"
exit 1