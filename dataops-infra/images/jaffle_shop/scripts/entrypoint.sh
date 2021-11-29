#!/usr/bin/env bash

aws s3 sync s3://new-bucket-dbt/jaffle_shop jaffle_shop
cd jaffle_shop
dbt debug
exec "$@"
