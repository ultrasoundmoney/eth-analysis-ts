import * as Blocks from "../blocks/blocks.js";
import { sql, sqlT, sqlTNotify, sqlTVoid } from "../db.js";
import * as FeeBurn from "../fee_burn.js";
import { A, pipe, T, TAlt, TOAlt } from "../fp.js";
import * as Log from "../log.js";
import { TimeFrameNext } from "../time_frames.js";

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

const getBurnCategoriesTimeFrame = (timeFrame: TimeFrameNext) =>
  pipe(
    Blocks.getEarliestBlockInTimeFrame(timeFrame),
    TOAlt.expect(
      `expect earliest block in time frame ${timeFrame} to exist for burn categories`,
    ),
    T.chain(
      (earliestBlock) => sqlT<BurnCategoryRow[]>`
        SELECT
          category,
          SUM(base_fees) AS fees,
          SUM(base_fees * eth_price / 1e18) AS fees_usd,
          SUM(transaction_count) AS transaction_count
        FROM contract_base_fees
        JOIN blocks ON number = block_number
        JOIN contracts ON address = contract_address
        WHERE category IS NOT NULL
        AND block_number >= ${earliestBlock}
        GROUP BY (category)
      `,
    ),
  );

const extendWithPercent = (
  feeBurn: FeeBurn.PreciseBaseFeeSum,
  burnCategories: BurnCategory[],
): BurnCategoryForCache[] =>
  pipe(
    burnCategories,
    A.map((burnCategory) => ({
      ...burnCategory,
      percentOfTotalBurn: burnCategory.fees / Number(feeBurn.eth),
      percentOfTotalBurnUsd: burnCategory.feesUsd / feeBurn.usd,
    })),
  );

const getBurnCategoriesWithPercent = (timeFrame: TimeFrameNext) =>
  pipe(
    T.Do,
    T.apS("feeBurns", FeeBurn.getFeeBurns()),
    T.apS("categories", getBurnCategoriesTimeFrame(timeFrame)),
    T.map(({ feeBurns, categories }) =>
      extendWithPercent(feeBurns[timeFrame], categories),
    ),
  );

export const updateBurnCategories = () =>
  pipe(
    TAlt.seqSPar({
      m5: getBurnCategoriesWithPercent("m5"),
      h1: getBurnCategoriesWithPercent("h1"),
      d1: getBurnCategoriesWithPercent("d1"),
      d7: getBurnCategoriesWithPercent("d7"),
      d30: getBurnCategoriesWithPercent("d30"),
      since_burn: getBurnCategoriesWithPercent("since_burn"),
    }),
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

// This query is slow. We only want to run one computation at a time with no queueing.
let isUpdating = false;

export const setIsUpdating = (nextIsUpdating: boolean) => {
  isUpdating = nextIsUpdating;
};

export const getIsUpdating = () => isUpdating;
