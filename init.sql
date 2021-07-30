CREATE TABLE "base_fees_per_block" (
  "hash" text PRIMARY KEY,
  "number" int UNIQUE NOT NULL,
  "base_fees" jsonb NOT NULL,
  "mined_at" timestamptz NOT NULL
);

CREATE TABLE "dapp_24h_totals" (
  "dapp_id" text,
  "contract_address" text,
  "fee_total" numeric,
  "oldest_included_block" int
);

CREATE TABLE "dapp_7d_totals" (
  "dapp_id" text,
  "contract_address" text,
  "fee_total" numeric NOT NULL,
  "oldest_included_block" int
);

CREATE TABLE "dapp_30d_totals" (
  "dapp_id" text,
  "contract_address" text,
  "fee_total" numeric NOT NULL,
  "oldest_included_block" int
);

CREATE TABLE "dapp_totals" (
  "dapp_id" text,
  "contract_address" text,
  "fee_total" numeric NOT NULL,
  "oldest_included_block" int
);

ALTER TABLE "dapp_24h_totals" ADD FOREIGN KEY ("oldest_included_block") REFERENCES "base_fees_per_block" ("number");

ALTER TABLE "dapp_30d_totals" ADD FOREIGN KEY ("oldest_included_block") REFERENCES "base_fees_per_block" ("number");

ALTER TABLE "dapp_7d_totals" ADD FOREIGN KEY ("oldest_included_block") REFERENCES "base_fees_per_block" ("number");

ALTER TABLE "dapp_totals" ADD FOREIGN KEY ("oldest_included_block") REFERENCES "base_fees_per_block" ("number");

CREATE INDEX ON "dapp_24h_totals" ("dapp_id");

CREATE INDEX ON "dapp_24h_totals" ("contract_address");

CREATE INDEX ON "dapp_7d_totals" ("dapp_id");

CREATE INDEX ON "dapp_7d_totals" ("contract_address");

CREATE INDEX ON "dapp_30d_totals" ("dapp_id");

CREATE INDEX ON "dapp_30d_totals" ("contract_address");

CREATE INDEX ON "dapp_totals" ("dapp_id");

CREATE INDEX ON "dapp_totals" ("contract_address");

COMMENT ON COLUMN "base_fees_per_block"."base_fees" IS 'document describing base fees burned';
