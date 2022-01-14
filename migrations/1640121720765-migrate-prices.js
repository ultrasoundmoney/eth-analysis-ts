export async function up(client) {
  await client.begin(async (sql) => {
    await sql`
      DROP TABLE eth_prices
    `;
    await sql`
      ALTER TABLE eth_prices_next
      RENAME TO eth_prices
    `;
  });
}
