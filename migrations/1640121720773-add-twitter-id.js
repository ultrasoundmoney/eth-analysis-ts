export async function up(client) {
  await client`
    ALTER TABLE contracts
      ADD COLUMN twitter_id text
  `;
}

export async function down(client) {
  await client`
    ALTER TABLE contracts
      DROP COLUMN twitter_id
  `;
}
