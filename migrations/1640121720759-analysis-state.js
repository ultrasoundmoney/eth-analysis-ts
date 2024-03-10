export async function up(client) {
  await client`
    ALTER TABLE "analysis_state" ADD COLUMN "first_analyzed_block" int;
  `;
}
