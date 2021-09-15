CREATE TYPE "timeframe" AS ENUM (
  '5m',
  '1h',
  '24h',
  '7d',
  '30d',
  'all'
);

CREATE TABLE "blocks" (
  "hash" text PRIMARY KEY,
  "number" int UNIQUE NOT NULL,
  "mined_at" timestamptz NOT NULL,
  "tips" float,
  "base_fee_sum" float,
  "contract_creation_sum" float,
  "eth_transfer_sum" float,
  "base_fee_per_gas" bigint,
  "gas_used" bigint
);

CREATE TABLE "contract_base_fees" (
  "block_number" int,
  "contract_address" text,
  "base_fees" float,
  PRIMARY KEY ("block_number", "contract_address")
);

CREATE TABLE "contracts" (
  "address" text PRIMARY KEY,
  "name" text,
  "last_metadata_fetch_at" timestamptz,
  "is_bot" boolean DEFAULT false,
  "dapp_id" text,
  "category" text,
  "twitter_handle" text,
  "image_url" text
);

CREATE TABLE "dapps" (
  "dapp_id" text PRIMARY KEY,
  "name" text
);

CREATE TABLE "derived_block_stats" (
  "block_number" int PRIMARY KEY,
  "burn_rates" jsonb,
  "fees_burned" jsonb,
  "leaderboards" jsonb
);

CREATE TABLE "contract_base_fee_sums" (
  "contract_address" text PRIMARY KEY,
  "base_fee_sum" float
);

CREATE TABLE "base_fee_sum_included_blocks" (
  "oldest_included_block" int,
  "newest_included_block" int,
  "timeframe" timeframe PRIMARY KEY
);

ALTER TABLE "contract_base_fees" ADD FOREIGN KEY ("block_number") REFERENCES "blocks" ("number");

ALTER TABLE "contract_base_fees" ADD FOREIGN KEY ("contract_address") REFERENCES "contracts" ("address");

ALTER TABLE "contracts" ADD FOREIGN KEY ("dapp_id") REFERENCES "dapps" ("dapp_id");

ALTER TABLE "derived_block_stats" ADD FOREIGN KEY ("block_number") REFERENCES "blocks" ("number");

ALTER TABLE "contract_base_fee_sums" ADD FOREIGN KEY ("contract_address") REFERENCES "contracts" ("address");

ALTER TABLE "base_fee_sum_included_blocks" ADD FOREIGN KEY ("oldest_included_block") REFERENCES "blocks" ("number");

ALTER TABLE "base_fee_sum_included_blocks" ADD FOREIGN KEY ("newest_included_block") REFERENCES "blocks" ("number");

CREATE INDEX ON "blocks" ("number");

CREATE INDEX ON "blocks" ("mined_at");
