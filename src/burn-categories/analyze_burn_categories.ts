import { sql, sqlT, sqlTVoid } from "../db.js";
import { pipe, T } from "../fp.js";
import * as Log from "../log.js";

Log.info("start analyzing burn categories");

// This query is slow. We only want to run one computation at a time with no queueing.
let isUpdating = false;

type BurnCategory = {
  category: string;
  fees: number;
  feesUsd: number;
  transactionCount: number;
};

export type BurnCategories = BurnCategory[];

const burnCategoriesCacheKey = "burn-categories-cache-key";

export const updateBurnCategories = () =>
  pipe(
    sqlT<BurnCategory[]>`
      SELECT
        category,
        SUM(base_fees) AS fees,
        SUM(base_fees * eth_price) AS fees_usd,
        SUM(transaction_count) AS transaction_count
      FROM contract_base_fees
      JOIN blocks ON number = block_number
      JOIN contracts ON address = contract_address
      WHERE category IS NOT NULL
      GROUP BY (category)
    `,
    T.chain(
      (burnCategories) =>
        sqlTVoid`
          INSERT INTO key_value_store
            ${sql({
              key: burnCategoriesCacheKey,
              value: JSON.stringify(burnCategories),
            })}
          ON CONFLICT (key) DO UPDATE SET
            value = excluded.value
        `,
    ),
    T.chainFirstIOK(() => () => {
      Log.debug("finished block analysis, waiting for next block update");
      isUpdating = false;
    }),
  );

sql.listen("blocks-update", () => {
  if (!isUpdating) {
    Log.debug("got blocks update, starting analysis");
    isUpdating = true;
    updateBurnCategories()();
    return;
  }

  Log.debug("got blocks update, but already analyzing, skipping block");
});
