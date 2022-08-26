export async function up(client) {
  await client`
		ALTER TABLE blocks
			ADD COLUMN difficulty int8
	`;
}

export async function down(client) {
  await client`
		ALTER TABLE blocks
			DROP COLUMN difficulty
	`;
}
