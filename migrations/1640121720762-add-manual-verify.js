export async function up(client) {
  await client`
    ALTER TABLE "contracts"
      ADD COLUMN last_manually_verified timestamptz;
  `;
}

export async function down(client) {
  await client`
    ALTER TABLE "contracts"
      DROP COLUMN last_manually_verified;
  `;
}
