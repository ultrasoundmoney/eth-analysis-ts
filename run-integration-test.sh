#!/bin/bash

TIMESTAMP=$(date +%s)

export PGHOST=localhost
export PGUSER=postgres
export PGPASSWORD=deflationary
export PGPORT=5432
export PGDATABASE=postgres
unset NODE_OPTIONS

psql --quiet --command "CREATE DATABASE test_$TIMESTAMP"

export PGDATABASE="test_$TIMESTAMP"

yarn ava --serial src/integration-test/blocks.test.ts

yarn ava --serial src/integration-test/burn_records.test.ts

yarn ava --serial src/integration-test/leaderboards.test.ts

yarn ava --serial src/integration-test/deflationary_streaks.test.ts

export PGDATABASE=postgres

psql --quiet --command "DROP DATABASE test_$TIMESTAMP"
