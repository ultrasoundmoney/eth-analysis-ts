export async function up(client) {
  const enum_types = await client`SELECT enum_range(NULL::timeframe)`;
  if (enum_types[0].enumRange.includes("since_merge")) {
    console.log("since_merge already exists in timeframe enum");
    return;
  } else {
    await client`
    ALTER TYPE timeframe ADD VALUE 'since_merge';
  `;
  }

  await client`
    CREATE TABLE contract_base_fee_sums_since_merge (
        base_fee_sum float8,
        base_fee_sum_usd float8,
        contract_address text PRIMARY KEY
    );
  `;
}

export async function down(client) {
  await client`
    DROP TABLE contract_base_fee_sums_since_merge;
  `;
}
