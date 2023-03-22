export async function up(client) {
  await client`
    ALTER TYPE timeframe ADD VALUE 'since_merge';
  `;

  await client`
    CREATE TABLE contract_base_fee_sums (
        base_fee_sum float8,
        base_fee_sum_usd float8,
        contract_address text PRIMARY KEY
    );
  `;
}

export async function down(client) {
  await client`
    DROP TABLE contract_base_fee_sums;
  `;
}
