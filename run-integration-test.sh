#!/bin/bash

TIMESTAMP=$(date +%s)

export PGHOST=localhost
export PGUSER=postgres
export PGPASSWORD=deflationary
export PGPORT=5432
export PGDATABASE=postgres

psql --quiet --command "CREATE DATABASE test_$TIMESTAMP"

export PGDATABASE="test_$TIMESTAMP"

node --loader ts-node/esm src/integration-test/blocks.test.ts

node --loader ts-node/esm src/integration-test/burn_records.test.ts

export PGDATABASE=postgres

psql --quiet --command "DROP DATABASE test_$TIMESTAMP"
