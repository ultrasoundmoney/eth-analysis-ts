export async function up(client) {
  await client`
    ALTER TABLE contract_base_fees
      ADD COLUMN base_fees_256 NUMERIC(78)
  `;
}

export async function down(client) {
  await client`
    ALTER TABLE contract_base_fees
      DROP COLUMN base_fees_256
  `;
}
