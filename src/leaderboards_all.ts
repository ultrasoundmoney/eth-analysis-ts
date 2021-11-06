import * as Blocks from "./blocks.js";
import * as Leaderboards from "./leaderboards.js";
import * as Log from "./log.js";
import { A, B, O, seqTParT, T } from "./fp.js";
import {
  ContractBaseFeesNext,
  LeaderboardEntry,
  LeaderboardRow,
} from "./leaderboards.js";
import { Row } from "postgres";
import { pipe } from "fp-ts/lib/function.js";
import { sql } from "./db.js";

type SyncStatus = "unknown" | "in-sync" | "out-of-sync";
let syncStatus: SyncStatus = "unknown";

export const getSyncStatus = (): SyncStatus => syncStatus;
export const setSyncStatus = (newSyncStatus: SyncStatus): void => {
  syncStatus = newSyncStatus;
};

export const getNewestIncludedBlockNumber = (): T.Task<O.Option<number>> =>
  pipe(
    () =>
      sql<{ newestIncludedBlock: number }[]>`
        SELECT newest_included_block
        FROM base_fee_sum_included_blocks
        WHERE timeframe = 'all'
      `,
    T.map((rows) => pipe(rows[0]?.newestIncludedBlock, O.fromNullable)),
  );

const setNewestIncludedBlockNumber =
  (blockNumber: number): T.Task<Row[]> =>
  () =>
    sql`
      INSERT INTO base_fee_sum_included_blocks (
        oldest_included_block,
        newest_included_block,
        timeframe
      )
      VALUES (
        ${Blocks.londonHardForkBlockNumber},
        ${blockNumber},
        'all'
      )
      ON CONFLICT (timeframe) DO UPDATE SET
        oldest_included_block = ${Blocks.londonHardForkBlockNumber},
        newest_included_block = ${blockNumber}
    `;

const addContractBaseFeeSums = (
  baseFeeSums: ContractBaseFeesNext,
): T.Task<void> =>
  pipe(
    baseFeeSums.size === 0,
    B.match(
      () =>
        pipe(
          Array.from(baseFeeSums.entries()),
          A.chunksOf(20000),
          A.map((entriesChunk) => ({
            addresses: entriesChunk.map((entry) => entry[0]),
            eths: entriesChunk.map((entry) => entry[1].eth),
            usds: entriesChunk.map((entry) => entry[1].usd),
          })),
          T.traverseArray(
            (toInsert) => () =>
              sql`
                  INSERT INTO contract_base_fee_sums (contract_address, base_fee_sum, base_fee_sum_usd)
                  SELECT
                    UNNEST(${sql.array(toInsert.addresses)}::text[]),
                    UNNEST(${sql.array(toInsert.eths)}::float[]),
                    UNNEST(${sql.array(toInsert.usds)}::float[])
                  ON CONFLICT (contract_address) DO UPDATE SET
                    base_fee_sum = contract_base_fee_sums.base_fee_sum + excluded.base_fee_sum::float,
                    base_fee_sum_usd = contract_base_fee_sums.base_fee_sum_usd + excluded.base_fee_sum_usd::float
                `,
          ),
          T.map(() => undefined),
        ),
      // Nothing to add.
      () => T.of(undefined),
    ),
  );

export const removeContractBaseFeeSums = (
  baseFeeSums: ContractBaseFeesNext,
): T.Task<void> => {
  return pipe(
    baseFeeSums.size === 0,
    B.match(
      () => {
        const addresses = Array.from(baseFeeSums.keys());
        const baseFeesEth = pipe(
          Array.from(baseFeeSums.values()),
          A.map((baseFeeSum) => baseFeeSum.eth),
        );
        const baseFeesUsd = pipe(
          Array.from(baseFeeSums.values()),
          A.map((baseFeeSum) => baseFeeSum.usd),
        );
        return pipe(
          () => sql`
                  UPDATE contract_base_fee_sums SET
                    base_fee_sum = base_fee_sum - data_table.base_fees,
                    base_fee_sum_usd = base_fee_sum_usd - data_table.base_fees_usd
                  FROM
                    (SELECT
                      UNNEST(${sql.array(
                        addresses,
                      )}::text[]) as contract_address,
                      UNNEST(${sql.array(baseFeesEth)}::float[]) as base_fees,
                      UNNEST(${sql.array(
                        baseFeesUsd,
                      )}::float[]) as base_fees_usd
                    ) as data_table
                  WHERE contract_base_fee_sums.contract_address = data_table.contract_address;
                `,
          T.map(() => undefined),
        );
      },
      // Nothing to remove.
      () => T.of(undefined),
    ),
  );
};

export const addBlock = (
  blockNumber: number,
  baseFeeSums: ContractBaseFeesNext,
): T.Task<void> =>
  pipe(
    seqTParT(
      addContractBaseFeeSums(baseFeeSums),
      setNewestIncludedBlockNumber(blockNumber),
    ),
    T.map(() => undefined),
  );

export const addMissingBlocks = (upToIncluding: number): T.Task<void> =>
  pipe(
    seqTParT(getNewestIncludedBlockNumber()),
    T.map(([newestIncludedBlockO]) => {
      return pipe(
        newestIncludedBlockO,
        O.getOrElse(() => {
          Log.info(
            "no newest included block for leaderboard 'all', assuming fresh start",
          );
          return Blocks.londonHardForkBlockNumber - 1;
        }),
      );
    }),
    T.chain((newestIncludedBlock) => {
      if (upToIncluding === newestIncludedBlock) {
        // All blocks already stored. Nothing to do.
        return T.of(undefined);
      }

      return pipe(
        Leaderboards.getRangeBaseFees(newestIncludedBlock + 1, upToIncluding),
        T.chain(addContractBaseFeeSums),
      );
    }),
  );

const getTopBaseFeeContracts = (): T.Task<LeaderboardRow[]> => {
  return () => sql<LeaderboardRow[]>`
    WITH top_base_fee_contracts AS (
      SELECT
        contract_address,
        base_fee_sum,
        base_fee_sum_usd
      FROM contract_base_fee_sums
      ORDER BY base_fee_sum DESC
      LIMIT 100
    )
    SELECT
      base_fee_sum AS base_fees,
      base_fee_sum_usd AS base_fees_usd,
      category,
      contract_address,
      image_url,
      is_bot,
      name,
      twitter_handle
    FROM top_base_fee_contracts
    JOIN contracts ON address = contract_address
  `;
};

export const calcLeaderboardAll = (): T.Task<LeaderboardEntry[]> => {
  return pipe(
    seqTParT(
      () => Leaderboards.getEthTransferFeesForTimeframe("all"),
      () => Leaderboards.getContractCreationBaseFeesForTimeframe("all"),
      pipe(
        getTopBaseFeeContracts(),
        T.chain(Leaderboards.extendRowsWithFamDetails),
      ),
    ),
    T.map(
      ([ethTransferBaseFees, contractCreationBaseFees, topBaseFeeContracts]) =>
        Leaderboards.buildLeaderboard(
          topBaseFeeContracts,
          ethTransferBaseFees,
          contractCreationBaseFees,
        ),
    ),
  );
};
