export async function up(client) {
  await client`
		ALTER TABLE blocks
			ADD COLUMN blob_gas_used bigint,
			ADD COLUMN blob_base_fee bigint,
			ADD COLUMN blob_fee_sum bigint,
			ADD COLUMN excess_blob_gas bigint
	`;
};

export async function down(client) {
  await client`
		ALTER TABLE blocks
            DROP COLUMN blob_gas_used,
			DROP COLUMN blob_base_fee,
			DROP COLUMN blob_fee_sum,
			DROP COLUMN excess_blob_gas
	`;
};
