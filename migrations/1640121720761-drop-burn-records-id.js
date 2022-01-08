export async function up(client) {
  await client`
    DROP TABLE "burn_records"
  `;

  await client`
    CREATE TABLE "burn_records" (
      "time_frame" text NOT NULL,
      "block_number" int NOT NULL,
      "base_fee_sum" float8 NOT NULL,
      PRIMARY KEY (time_frame, block_number)
    )
  `;

  await client`
    ALTER TABLE "burn_records"
    ADD FOREIGN KEY ("block_number")
    REFERENCES "blocks" ("number")
  `;
}
