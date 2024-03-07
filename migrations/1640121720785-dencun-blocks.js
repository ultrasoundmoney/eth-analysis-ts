export async function up(client) {
  await client`
		ALTER TABLE blocks
			ADD COLUMN blob_gas_used bigint,
			ADD COLUMN excess_blob_gas bigint
	`;
};

export async function down(client) {
  await client`
		ALTER TABLE blocks
            DROP COLUMN blob_gas_used,
			ADD COLUMN excess_blob_gas
	`;
};
