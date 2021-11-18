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
  "tips" double,
  "base_fee_sum" double,
  "contract_creation_sum" double,
  "eth_transfer_sum" double,
  "base_fee_per_gas" bigint,
  "gas_used" bigint,
  "eth_price" double
);

CREATE TABLE "contract_base_fees" (
  "block_number" int,
  "contract_address" text,
  "base_fees" double,
  PRIMARY KEY ("block_number", "contract_address")
);

CREATE TABLE "contracts" (
  "address" text PRIMARY KEY,
  "name" text,
  "is_bot" boolean DEFAULT false,
  "category" text,
  "twitter_handle" text,
  "image_url" text,
  "etherscan_name_tag" text,
  "etherscan_name_token" text,
  "opensea_contract_last_fetch" timestamptz,
  "opensea_name" text,
  "opensea_twitter_handle" text,
  "opensea_category" text,
  "opensea_schema_name" text,
  "opensea_image_url" text,
  "defi_llama_twitter_handle" text,
  "defi_llama_category" text,
  "manual_name" text,
  "manual_twitter_handle" text,
  "manual_category" text,
  "twitter_image_url" text,
  "twitter_name" text,
  "twitter_description" text,
  "web3_name" text,
  "web3_supports_eip_721" boolean,
  "web3_supports_eip_1155" boolean,
  "mined_at_block" int,
  "mined_at" timestamptz,
  "force_metadata_fetch" boolean
);

CREATE TABLE "derived_block_stats" (
  "block_number" int PRIMARY KEY,
  "burn_rates" jsonb,
  "fees_burned" jsonb,
  "leaderboards" jsonb
);

CREATE TABLE "contract_base_fee_sums" (
  "contract_address" text PRIMARY KEY,
  "base_fee_sum" double,
  "base_fee_sum_usd" double
);

CREATE TABLE "base_fee_sum_included_blocks" (
  "oldest_included_block" int,
  "newest_included_block" int,
  "timeframe" timeframe PRIMARY KEY
);

CREATE TABLE "contract_creations" (
  "address" text,
  "block_number" int
);

CREATE TABLE "eth_prices" (
  "timestamp" timestamptz PRIMARY KEY,
  "ethusd" double
);

ALTER TABLE "contract_base_fees" ADD FOREIGN KEY ("block_number") REFERENCES "blocks" ("number");

ALTER TABLE "contract_base_fees" ADD FOREIGN KEY ("contract_address") REFERENCES "contracts" ("address");

ALTER TABLE "contracts" ADD FOREIGN KEY ("mined_at_block") REFERENCES "blocks" ("number");

ALTER TABLE "derived_block_stats" ADD FOREIGN KEY ("block_number") REFERENCES "blocks" ("number");

ALTER TABLE "contract_base_fee_sums" ADD FOREIGN KEY ("contract_address") REFERENCES "contracts" ("address");

ALTER TABLE "base_fee_sum_included_blocks" ADD FOREIGN KEY ("oldest_included_block") REFERENCES "blocks" ("number");

ALTER TABLE "base_fee_sum_included_blocks" ADD FOREIGN KEY ("newest_included_block") REFERENCES "blocks" ("number");

CREATE INDEX ON "blocks" ("number");

CREATE INDEX ON "blocks" ("mined_at");

CREATE INDEX ON "contracts" ("force_metadata_fetch");
