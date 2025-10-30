export async function up(client) {
  await client`
    ALTER TABLE public.blocks
      ALTER COLUMN blob_fee_sum TYPE numeric(78) USING blob_fee_sum::numeric,
      ALTER COLUMN blob_base_fee TYPE numeric(78) USING blob_base_fee::numeric
  `;

  await client`
    ALTER TABLE public.burn_records
      ALTER COLUMN blob_fee_sum TYPE numeric(78) USING blob_fee_sum::numeric
  `;
}

export async function down(client) {
  await client`
    ALTER TABLE public.blocks
      ALTER COLUMN blob_fee_sum TYPE bigint USING (blob_fee_sum::bigint),
      ALTER COLUMN blob_base_fee TYPE bigint USING (blob_base_fee::bigint)
  `;

  await client`
    ALTER TABLE public.burn_records
      ALTER COLUMN blob_fee_sum TYPE bigint USING (blob_fee_sum::bigint)
  `;
}


