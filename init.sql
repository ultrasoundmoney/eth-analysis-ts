CREATE TABLE "base_fees_per_block" (
  "hash" text PRIMARY KEY,
  "number" int UNIQUE NOT NULL,
  "base_fees" jsonb NOT NULL,
  "mined_at" timestamptz NOT NULL,
  "tips" bigint,
  "base_fee_sum" bigint
);

CREATE TABLE "dapp_1h_totals" (
  "dapp_id" text PRIMARY KEY,
  "fee_total" numeric,
  "oldest_included_block" int NOT NULL
);

CREATE TABLE "dapp_24h_totals" (
  "dapp_id" text PRIMARY KEY,
  "fee_total" numeric,
  "oldest_included_block" int NOT NULL
);

CREATE TABLE "dapp_7d_totals" (
  "dapp_id" text PRIMARY KEY,
  "fee_total" numeric NOT NULL,
  "oldest_included_block" int NOT NULL
);

CREATE TABLE "dapp_30d_totals" (
  "dapp_id" text PRIMARY KEY,
  "fee_total" numeric NOT NULL,
  "oldest_included_block" int NOT NULL
);

CREATE TABLE "dapp_all_totals" (
  "dapp_id" text PRIMARY KEY,
  "fee_total" numeric NOT NULL,
  "oldest_included_block" int NOT NULL
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
  "has_image" boolean,
  "served_count" integer DEFAULT 0,
  "is_bot" boolean DEFAULT false,
  "dapp_id" text
);

CREATE TABLE "dapps" (
  "dapp_id" text PRIMARY KEY,
  "name" text,
  "has_image" boolean
);

ALTER TABLE "dapp_1h_totals" ADD FOREIGN KEY ("dapp_id") REFERENCES "dapps" ("dapp_id");

ALTER TABLE "dapp_1h_totals" ADD FOREIGN KEY ("oldest_included_block") REFERENCES "base_fees_per_block" ("number");

ALTER TABLE "dapp_24h_totals" ADD FOREIGN KEY ("dapp_id") REFERENCES "dapps" ("dapp_id");

ALTER TABLE "dapp_24h_totals" ADD FOREIGN KEY ("oldest_included_block") REFERENCES "base_fees_per_block" ("number");

ALTER TABLE "dapp_7d_totals" ADD FOREIGN KEY ("dapp_id") REFERENCES "dapps" ("dapp_id");

ALTER TABLE "dapp_7d_totals" ADD FOREIGN KEY ("oldest_included_block") REFERENCES "base_fees_per_block" ("number");

ALTER TABLE "dapp_30d_totals" ADD FOREIGN KEY ("dapp_id") REFERENCES "dapps" ("dapp_id");

ALTER TABLE "dapp_30d_totals" ADD FOREIGN KEY ("oldest_included_block") REFERENCES "base_fees_per_block" ("number");

ALTER TABLE "dapp_all_totals" ADD FOREIGN KEY ("dapp_id") REFERENCES "dapps" ("dapp_id");

ALTER TABLE "dapp_all_totals" ADD FOREIGN KEY ("oldest_included_block") REFERENCES "base_fees_per_block" ("number");

ALTER TABLE "contract_1h_totals" ADD FOREIGN KEY ("contract_address") REFERENCES "contracts" ("address");

ALTER TABLE "contract_1h_totals" ADD FOREIGN KEY ("oldest_included_block") REFERENCES "base_fees_per_block" ("number");

ALTER TABLE "contract_24h_totals" ADD FOREIGN KEY ("contract_address") REFERENCES "contracts" ("address");

ALTER TABLE "contract_24h_totals" ADD FOREIGN KEY ("oldest_included_block") REFERENCES "base_fees_per_block" ("number");

ALTER TABLE "contract_7d_totals" ADD FOREIGN KEY ("contract_address") REFERENCES "contracts" ("address");

ALTER TABLE "contract_7d_totals" ADD FOREIGN KEY ("oldest_included_block") REFERENCES "base_fees_per_block" ("number");

ALTER TABLE "contract_30d_totals" ADD FOREIGN KEY ("contract_address") REFERENCES "contracts" ("address");

ALTER TABLE "contract_30d_totals" ADD FOREIGN KEY ("oldest_included_block") REFERENCES "base_fees_per_block" ("number");

ALTER TABLE "contract_all_totals" ADD FOREIGN KEY ("contract_address") REFERENCES "contracts" ("address");

ALTER TABLE "contract_all_totals" ADD FOREIGN KEY ("oldest_included_block") REFERENCES "base_fees_per_block" ("number");

ALTER TABLE "contracts" ADD FOREIGN KEY ("dapp_id") REFERENCES "dapps" ("dapp_id");

CREATE INDEX ON "base_fees_per_block" ("number");

CREATE INDEX ON "base_fees_per_block" ("mined_at");
