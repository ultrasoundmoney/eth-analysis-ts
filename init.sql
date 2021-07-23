CREATE TABLE "blocks" (
  "hash" text PRIMARY KEY,
  "number" int UNIQUE NOT NULL,
  "json" jsonb NOT NULL
);

CREATE TABLE "transaction_receipts" (
  "hash" text PRIMARY KEY,
  "json" jsonb NOT NULL
);

CREATE INDEX ON "blocks" ("number");
