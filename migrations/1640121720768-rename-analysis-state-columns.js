export async function up(client) {
  await client`
    ALTER TABLE analysis-state
      RENAME COLUMN first_analyzed_block TO first
  `;
  await client`
    ALTER TABLE analysis-state
      RENAME COLUMN last_analyzed_block TO last
  `;
}

export async function down(client) {
  await client`
    ALTER TABLE analysis-state
      RENAME COLUMN first TO first_analyzed_block
  `;
  await client`
    ALTER TABLE analysis-state
      RENAME COLUMN last TO last_analyzed_block
  `;
}
