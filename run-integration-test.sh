#!/bin/bash

set -e

TIMESTAMP=$(date +%s)

export PGHOST=localhost
export PGUSER=postgres
export PGPASSWORD=deflationary
export PGPORT=5432
export PGDATABASE=postgres

psql --quiet --command "CREATE DATABASE test_$TIMESTAMP"

export PGDATABASE="test_$TIMESTAMP"

psql --quiet --file init.sql

node --loader ts-node/esm src/integration-test/analyze_blocks.test.ts

psql --quiet --command "DROP DATABASE test_$TIMESTAMP"
