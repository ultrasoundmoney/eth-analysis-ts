export async function up(client) {
  await client`
    ALTER TABLE contract_base_fees
     ADD COLUMN transaction_count float8
  `;
}

export async function down(client) {
  await client`
    ALTER TABLE contract_base_fees
      DROP COLUMN transaction_count
  `;
}
