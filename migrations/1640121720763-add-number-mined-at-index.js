export async function up(client) {
  await client`
    CREATE UNIQUE INDEX blocks_number_mined_at_idx ON public.blocks ("number",mined_at)
  `;
}

export async function down(client) {
  await client`
    DROP INDEX blocks_number_mined_at_idx
  `;
}
