export async function up(client) {
  await client`
    CREATE TABLE eth_prices_next (
      timestamp timestamptz PRIMARY KEY,
      ethusd float8
    )
  `;
}

export async function down(client) {
  await client`
    DROP table eth_prices_next
  `;
}
