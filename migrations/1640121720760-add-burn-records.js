export async function up(client) {
  await client`
    CREATE TABLE "burn_records" (
      "burn_record_id" serial PRIMARY KEY,
      "time_frame" text NOT NULL,
      "block_number" int NOT NULL,
      "base_fee_sum" float8 NOT NULL,
      UNIQUE (time_frame, block_number)
    )
  `;

  await client`
    ALTER TABLE "burn_records"
    ADD FOREIGN KEY ("block_number")
    REFERENCES "blocks" ("number")
  `;
}
