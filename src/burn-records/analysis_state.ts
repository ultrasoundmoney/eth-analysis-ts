import { sql } from "../db.js";

export const getLastAnalyzedBlockNumber = async (): Promise<
  number | undefined
> => {
  const rows = await sql<{ lastAnalyzedBlock: number }[]>`
    SELECT last_analyzed_block FROM analysis_state
    WHERE key = 'burn_records_all'
  `;

  return rows[0]?.lastAnalyzedBlock ?? undefined;
};
