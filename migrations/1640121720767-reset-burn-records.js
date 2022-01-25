export async function up(client) {
  await client`
    DELETE FROM analysis_state
    WHERE key = 'burn_records'
  `;

  await client`
    TRUNCATE burn_records
  `;
}
