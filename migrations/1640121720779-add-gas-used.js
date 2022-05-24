export async function up(client) {
  await client`
    ALTER TABLE public.contract_base_fees ADD gas_used numeric(78) NULL
  `;
}

export async function down(client) {
  await client`
    ALTER TABLE public.contract_base_fees DROP COLUMN gas_used
  `;
}
