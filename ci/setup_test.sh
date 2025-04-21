#!/usr/bin/env bash
set -eux

max_retries=12
retry_count=0
container_name='opengauss'
image='opengauss/opengauss:7.0.0-RC1.B023'
password='Gaussdb@123!'
mount_dir='/home/omm/tmp'

#groupadd -g 1700 omm
#useradd -u 1700 -g omm -m -s /bin/bash omm

docker rm -f "${container_name}" 2>/dev/null || true
rm -rf "${mount_dir}/*" && mkdir -p "${mount_dir}" && cp -rf testsetup/opengauss_setup.sql "${mount_dir}/"

docker run \
  --name "${container_name}" \
  --privileged=true \
  -d \
  -e GS_PASSWORD=${password} \
  -v ${mount_dir}:/tmp \
  -p 5432:5432 ${image}

while [ $retry_count -lt $max_retries ]; do
    if docker exec -it "${container_name}" bash -c "su - omm -c 'gsql -U omm -c \"select 1;\"'" >/dev/null 2>&1; then
      docker exec -it "${container_name}" bash -c "su - omm -c 'gsql -U omm -c \"CREATE DATABASE pgx_test DBCOMPATIBILITY 'pg';\" -f /tmp/opengauss_setup.sql'"
      echo "Database initialization completed."
      exit 0
    fi
    echo "Waiting for database to be ready... (attempt $((retry_count + 1))/$max_retries)"
    sleep 5
    ((++retry_count))
done