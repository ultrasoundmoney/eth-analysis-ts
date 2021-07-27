CREATE TABLE "base_fees_per_block" (
  "hash" text PRIMARY KEY,
  "number" int UNIQUE NOT NULL,
  "base_fees" jsonb NOT NULL
);

COMMENT ON COLUMN "base_fees_per_block"."base_fees" IS 'document describing base fees burned';
