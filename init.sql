CREATE TYPE "timeframe" AS ENUM (
  '5m',
  '1h',
  '24h',
  '7d',
  '30d',
  'all'
);

CREATE TABLE "blocks" (
  "base_fee_per_gas" bigint,
  "base_fee_sum" float8,
  "base_fee_sum_256" numeric(78),
  "contract_creation_sum" float8,
  "eth_price" float8,
  "eth_transfer_sum" float8,
  "gas_used" bigint,
  "hash" text PRIMARY KEY,
  "mined_at" timestamptz NOT NULL,
  "number" int UNIQUE NOT NULL,
  "tips" float8
);

CREATE TABLE "contract_base_fees" (
  "base_fees" float8,
  "block_number" int,
  "contract_address" text,
  PRIMARY KEY ("block_number", "contract_address")
);

CREATE TABLE "contracts" (
  "address" text PRIMARY KEY,
  "category" text,
  "defi_llama_category" text,
  "defi_llama_twitter_handle" text,
  "etherscan_name_tag" text,
  "etherscan_name_token" text,
  "force_metadata_fetch" boolean,
  "image_url" text,
  "is_bot" boolean DEFAULT false,
  "manual_category" text,
  "manual_name" text,
  "manual_twitter_handle" text,
  "mined_at" timestamptz,
  "mined_at_block" int,
  "name" text,
  "opensea_category" text,
  "opensea_contract_last_fetch" timestamptz,
  "opensea_image_url" text,
  "opensea_name" text,
  "opensea_schema_name" text,
  "opensea_twitter_handle" text,
  "twitter_description" text,
  "twitter_handle" text,
  "twitter_image_url" text,
  "twitter_name" text,
  "web3_name" text,
  "web3_supports_eip_1155" boolean,
  "web3_supports_eip_721" boolean
);

CREATE TABLE "derived_block_stats" (
  "block_number" int PRIMARY KEY,
  "burn_rates" jsonb,
  "fees_burned" jsonb,
  "leaderboards" jsonb
);

CREATE TABLE "contract_base_fee_sums" (
  "base_fee_sum" float8,
  "base_fee_sum_usd" float8,
  "contract_address" text PRIMARY KEY
);

CREATE TABLE "base_fee_sum_included_blocks" (
  "newest_included_block" int,
  "oldest_included_block" int,
  "timeframe" timeframe PRIMARY KEY
);

CREATE TABLE "eth_prices" (
  "ethusd" float8,
  "ethusd_24h_change" float8,
  "timestamp" timestamptz PRIMARY KEY
);

CREATE TABLE "market_caps" (
  "btc_market_cap" float8,
  "eth_market_cap" float8,
  "gold_market_cap" float8,
  "timestamp" timestamptz PRIMARY KEY,
  "usd_m2_market_cap" float8
);

CREATE TABLE "analysis_state" (
  "key" text PRIMARY KEY,
  "last_analyzed_block" int
);

CREATE TABLE "fee_records" (
  "denomination" text,
  "fee_sum" numeric(78),
  "first_block" int,
  "granularity" text,
  "last_block" int,
  "sorting" text
);

ALTER TABLE "contract_base_fees" ADD FOREIGN KEY ("block_number") REFERENCES "blocks" ("number");

ALTER TABLE "contract_base_fees" ADD FOREIGN KEY ("contract_address") REFERENCES "contracts" ("address");

ALTER TABLE "contracts" ADD FOREIGN KEY ("mined_at_block") REFERENCES "blocks" ("number");

ALTER TABLE "derived_block_stats" ADD FOREIGN KEY ("block_number") REFERENCES "blocks" ("number");

ALTER TABLE "contract_base_fee_sums" ADD FOREIGN KEY ("contract_address") REFERENCES "contracts" ("address");

ALTER TABLE "base_fee_sum_included_blocks" ADD FOREIGN KEY ("newest_included_block") REFERENCES "blocks" ("number");

ALTER TABLE "base_fee_sum_included_blocks" ADD FOREIGN KEY ("oldest_included_block") REFERENCES "blocks" ("number");

ALTER TABLE "analysis_state" ADD FOREIGN KEY ("last_analyzed_block") REFERENCES "blocks" ("number");

ALTER TABLE "fee_records" ADD FOREIGN KEY ("first_block") REFERENCES "blocks" ("number");

ALTER TABLE "fee_records" ADD FOREIGN KEY ("last_block") REFERENCES "blocks" ("number");

CREATE INDEX ON "blocks" ("number");

CREATE INDEX ON "blocks" ("mined_at");

CREATE INDEX ON "contracts" ("force_metadata_fetch");
