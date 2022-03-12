export async function up(client) {
  await client`
    INSERT INTO analysis_state (key, last)
    SELECT 'burn-records', last FROM analysis_state
    WHERE key = 'burn_records'
    ON CONFLICT DO NOTHING
  `;

  await client`
    DELETE FROM analysis_state
    WHERE key = 'burn_records'
  `;
}

export async function down(client) {
  await client`
    INSERT INTO analysis_state (key, last)
    SELECT 'burn_records', last FROM analysis_state
    WHERE key = 'burn-records'
    ON CONFLICT DO NOTHING
  `;

  await client`
    DELETE FROM analysis_state
    WHERE key = 'burn-records'
  `;
}
