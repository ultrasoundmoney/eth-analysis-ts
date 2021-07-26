CREATE TABLE "fees_per_block" (
  "hash" text PRIMARY KEY,
  "number" int UNIQUE NOT NULL,
  "fees_paid" jsonb
);

COMMENT ON COLUMN "fees_per_block"."fees_paid" IS 'document describing fees paid';
