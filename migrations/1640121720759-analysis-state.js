export async function up(client) {
  await client`
    CREATE TABLE "analysis_state" (
      "key" text PRIMARY KEY,
      "first_analyzed_block" int,
      "last_analyzed_block" int
    );
  `;
}
