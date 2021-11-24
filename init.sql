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
  "base_fee_sum" double precision,
  "contract_creation_sum" double precision,
  "eth_price" double precision,
  "eth_transfer_sum" double precision,
  "gas_used" bigint,
  "hash" text PRIMARY KEY,
  "mined_at" timestamptz NOT NULL,
  "number" int UNIQUE NOT NULL,
  "tips" double precision
);

CREATE TABLE "contract_base_fees" (
  "base_fees" double precision,
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
  "base_fee_sum" double precision,
  "base_fee_sum_usd" double precision,
  "contract_address" text PRIMARY KEY
);

CREATE TABLE "base_fee_sum_included_blocks" (
  "newest_included_block" int,
  "oldest_included_block" int,
  "timeframe" timeframe PRIMARY KEY
);

CREATE TABLE "contract_creations" (
  "address" text,
  "block_number" int
);

CREATE TABLE "eth_prices" (
  "ethusd" double precision,
  "ethusd_24h_change" double precision,
  "timestamp" timestamptz PRIMARY KEY
);

CREATE TABLE "market_caps" (
  "btc_market_cap" double precision,
  "eth_market_cap" double precision,
  "gold_market_cap" double precision,
  "timestamp" timestamptz PRIMARY KEY,
  "usd_m3_market_cap" double precision
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
