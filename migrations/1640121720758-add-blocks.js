export async function up(client) {
  await client`
    CREATE TABLE IF NOT EXISTS "blocks" (
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
    )
  `;

  await client`
    CREATE INDEX ON "blocks" ("number");
  `;

  await client`
    CREATE INDEX ON "blocks" ("mined_at");
  `;
}
