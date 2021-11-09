import * as A from "fp-ts/lib/Array.js";
import { pipe } from "fp-ts/lib/function.js";
import { sql } from "./db.js";
import * as Log from "./log.js";

export type FeesBurnedPerInterval = Record<string, number>;

export const getFeesBurnedPerInterval =
  async (): Promise<FeesBurnedPerInterval> => {
    const blocks = await sql<{ baseFeeSum: number | null; date: Date }[]>`
      SELECT date_trunc('hour', mined_at) AS date, SUM(base_fee_sum) AS base_fee_sum
      FROM blocks
      GROUP BY date
      ORDER BY date
    `.then((rows) => {
      if (rows.length === 0) {
        Log.warn(
          "tried to determine base fees per day, but found no analyzed blocks",
        );
      }

      return rows;
    });

    if (blocks.length === 0) {
      return {};
    }

    return pipe(
      blocks,
      A.map(({ baseFeeSum, date }) => [date.getTime() / 1000, baseFeeSum ?? 0]),
      Object.fromEntries,
    );
  };
