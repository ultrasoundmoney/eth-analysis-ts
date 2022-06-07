export async function up(client) {
  await client`
		UPDATE contracts
		SET category = 'mev', is_bot = TRUE
		WHERE name ILIKE 'mev bot:%'
	`;
}

export async function down() {}
