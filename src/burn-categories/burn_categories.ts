import { sql, sqlT, sqlTNotify, sqlTVoid } from "../db.js";
import * as FeeBurn from "../fee_burn.js";
import { pipe, T } from "../fp.js";
import * as Log from "../log.js";
import { setIsUpdating } from "./analyze_burn_categories.js";

type BurnCategoryRow = {
  category: string;
  fees: number;
  feesUsd: number;
  transactionCount: number;
};

type BurnCategory = {
  category: string;
  fees: number;
  feesUsd: number;
  transactionCount: number;
};

export type BurnCategories = BurnCategory[];

type BurnCategoryForCache = {
  category: string;
  fees: number;
  feesUsd: number;
  percentOfTotalBurn: number;
  percentOfTotalBurnUsd: number;
  transactionCount: number;
};

type BurnCategoriesCache = BurnCategoryForCache[];

export const burnCategoriesCacheKey = "burn-categories-cache-key";

const getBurnCategories = () =>
  pipe(
    sqlT<BurnCategoryRow[]>`
      SELECT
        category,
        SUM(base_fees) AS fees,
        SUM(base_fees * eth_price / 1e18) AS fees_usd,
        SUM(transaction_count) AS transaction_count
      FROM contract_base_fees
      JOIN blocks ON number = block_number
      JOIN contracts ON address = contract_address
      WHERE category IS NOT NULL
      GROUP BY (category)
    `,
  );

export const updateBurnCategories = () =>
  pipe(
    T.Do,
    T.apS("feeBurn", FeeBurn.getFeeBurnAll()),
    T.apS("burnCategories", getBurnCategories()),
    T.map(({ feeBurn, burnCategories }) =>
      burnCategories.map((burnCategory) => ({
        ...burnCategory,
        percentOfTotalBurn: burnCategory.fees / Number(feeBurn.eth),
        percentOfTotalBurnUsd: burnCategory.feesUsd / feeBurn.usd,
      })),
    ),
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
    T.chain(() => sqlTNotify("cache-update", burnCategoriesCacheKey)),
    T.chainFirstIOK(() => () => {
      Log.debug("finished block analysis, waiting for next block update");
      setIsUpdating(false);
    }),
  );

export const getCategoriesCache = () =>
  pipe(
    sqlT<{ value: BurnCategoriesCache }[]>`
      SELECT value FROM key_value_store
      WHERE key = ${burnCategoriesCacheKey}
    `,
    T.map((rows) => rows[0]?.value),
  );

console.log(await getCategoriesCache()());
