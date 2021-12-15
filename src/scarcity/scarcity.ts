import { sql } from "../db.js";

export type ScarcityJson = string;

export const getLatestScarcity = async (): Promise<ScarcityJson> => {
  const rows = await sql<{ scarcity: string }[]>`
    SELECT scarcity FROM derived_block_stats
    WHERE block_number = (
      SELECT MAX(block_number) FROM derived_block_stats
    )
  `;

  return rows[0]?.scarcity;
};
