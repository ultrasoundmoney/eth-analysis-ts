export async function up(client) {
  await client`
    ALTER TABLE contracts
      ADD COLUMN coingecko_categories _text NULL,
      ADD COLUMN coingecko_image_url text NULL,
      ADD COLUMN coingecko_last_fetch TIMESTAMPTZ NULL,
      ADD COLUMN coingecko_name text NULL,
      ADD COLUMN coingecko_total_attempts int NOT NULL DEFAULT 0,
      ADD COLUMN coingecko_twitter_handle text NULL;
  `;
}

export async function down(client) {
  await client`
    ALTER TABLE contracts
      DROP COLUMN coingecko_categories,
      DROP COLUMN coingecko_image_url,
      DROP COLUMN coingecko_last_fetch,
      DROP COLUMN coingecko_name,
      DROP COLUMN coingecko_total_attempts,
      DROP COLUMN coingecko_twitter_handle;
  `;
}
