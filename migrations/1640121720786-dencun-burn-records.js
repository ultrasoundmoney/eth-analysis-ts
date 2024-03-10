export async function up(client) {
  await client`
		ALTER TABLE burn_records
            ADD COLUMN blob_fee_sum bigint;
	`;
};

export async function down(client) {
  await client`
		ALTER TABLE burn_records
            DROP COLUMN blob_fee_sum;
	`;
};
