export async function up(client) {
  await client`
    DROP TABLE eth_prices
  `;
  await client`
    ALTER TABLE eth_prices_next
    RENAME TO eth_prices
  `;
}
