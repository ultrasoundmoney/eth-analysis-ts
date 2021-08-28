CREATE TABLE "blocks" (
  "hash" text PRIMARY KEY,
  "number" int UNIQUE NOT NULL,
  "base_fees" jsonb NOT NULL,
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

CREATE TABLE "contract_1h_totals" (
  "contract_address" text PRIMARY KEY,
  "fee_total" numeric NOT NULL,
  "oldest_included_block" int NOT NULL
);

CREATE TABLE "contract_24h_totals" (
  "contract_address" text PRIMARY KEY,
  "fee_total" numeric NOT NULL,
  "oldest_included_block" int NOT NULL
);

CREATE TABLE "contract_7d_totals" (
  "contract_address" text PRIMARY KEY,
  "fee_total" numeric NOT NULL,
  "oldest_included_block" int NOT NULL
);

CREATE TABLE "contract_30d_totals" (
  "contract_address" text PRIMARY KEY,
  "fee_total" numeric NOT NULL,
  "oldest_included_block" int NOT NULL
);

CREATE TABLE "contract_all_totals" (
  "contract_address" text PRIMARY KEY,
  "fee_total" numeric NOT NULL,
  "oldest_included_block" int NOT NULL
);

CREATE TABLE "contracts" (
  "address" text PRIMARY KEY,
  "name" text,
  "last_name_fetch_at" timestamptz,
  "last_metadata_fetch_at" timestamptz,
  "is_bot" boolean DEFAULT false,
  "dapp_id" text
);

CREATE TABLE "dapps" (
  "dapp_id" text PRIMARY KEY,
  "name" text
);

CREATE TABLE "derived_block_stats" (
  "number" int PRIMARY KEY,
  "burn_rates" jsonb,
  "fees_burned" jsonb,
  "leaderboards" jsonb
);

ALTER TABLE "contract_base_fees" ADD FOREIGN KEY ("block_number") REFERENCES "blocks" ("number");

ALTER TABLE "contract_base_fees" ADD FOREIGN KEY ("contract_address") REFERENCES "contracts" ("address");

ALTER TABLE "contract_1h_totals" ADD FOREIGN KEY ("contract_address") REFERENCES "contracts" ("address");

ALTER TABLE "contract_1h_totals" ADD FOREIGN KEY ("oldest_included_block") REFERENCES "blocks" ("number");

ALTER TABLE "contract_24h_totals" ADD FOREIGN KEY ("contract_address") REFERENCES "contracts" ("address");

ALTER TABLE "contract_24h_totals" ADD FOREIGN KEY ("oldest_included_block") REFERENCES "blocks" ("number");

ALTER TABLE "contract_7d_totals" ADD FOREIGN KEY ("contract_address") REFERENCES "contracts" ("address");

ALTER TABLE "contract_7d_totals" ADD FOREIGN KEY ("oldest_included_block") REFERENCES "blocks" ("number");

ALTER TABLE "contract_30d_totals" ADD FOREIGN KEY ("contract_address") REFERENCES "contracts" ("address");

ALTER TABLE "contract_30d_totals" ADD FOREIGN KEY ("oldest_included_block") REFERENCES "blocks" ("number");

ALTER TABLE "contract_all_totals" ADD FOREIGN KEY ("contract_address") REFERENCES "contracts" ("address");

ALTER TABLE "contract_all_totals" ADD FOREIGN KEY ("oldest_included_block") REFERENCES "blocks" ("number");

ALTER TABLE "contracts" ADD FOREIGN KEY ("dapp_id") REFERENCES "dapps" ("dapp_id");

ALTER TABLE "derived_block_stats" ADD FOREIGN KEY ("number") REFERENCES "blocks" ("number");

CREATE INDEX ON "blocks" ("number");

CREATE INDEX ON "blocks" ("mined_at");
