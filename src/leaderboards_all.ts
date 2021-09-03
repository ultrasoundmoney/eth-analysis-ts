import * as Blocks from "./blocks.js";
import * as Leaderboards from "./leaderboards.js";
import * as Log from "./log.js";
import { A, B, E, O, T, TE } from "./fp.js";
import {
  AddedBaseFeesLog,
  ContractBaseFees,
  LeaderboardEntry,
  LeaderboardRow,
} from "./leaderboards.js";
import { Row } from "postgres";
import { pipe } from "fp-ts/lib/function.js";
import { seqTPar, seqTSeq } from "./sequence.js";
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
      INSERT INTO base_fee_sum_included_blocks (oldest_included_block, newest_included_block, timeframe)
      VALUES (${Blocks.londonHardForkBlockNumber}, ${blockNumber}, 'all')
      ON CONFLICT (timeframe) DO UPDATE SET
      oldest_included_block = ${Blocks.londonHardForkBlockNumber},
      newest_included_block = ${blockNumber}
    `;

const addContractBaseFeeSums = (baseFeeSums: ContractBaseFees): T.Task<void> =>
  pipe(
    baseFeeSums.size === 0,
    B.match(
      () =>
        pipe(
          Array.from(baseFeeSums.entries()),
          A.chunksOf(20000),
          A.map(A.unzip),
          T.traverseArray(
            ([addresses, baseFees]) =>
              () =>
                sql`
                  INSERT INTO contract_base_fee_sums (contract_address, base_fee_sum)
                  SELECT
                    UNNEST(${sql.array(addresses)}::text[]),
                    UNNEST(${sql.array(baseFees)}::float[])
                  ON CONFLICT (contract_address) DO UPDATE SET
                  base_fee_sum = contract_base_fee_sums.base_fee_sum + excluded.base_fee_sum::float
                `,
          ),
          T.map(() => undefined),
        ),
      // Nothing to add.
      () => T.of(undefined),
    ),
  );

const removeContractBaseFeeSums = (
  baseFeeSums: ContractBaseFees,
): T.Task<void> => {
  return pipe(
    baseFeeSums.size === 0,
    B.match(
      () => {
        const addresses = Array.from(baseFeeSums.keys());
        const baseFees = Array.from(baseFeeSums.values());
        return pipe(
          () => sql`
                  UPDATE contract_base_fee_sums
                    SET base_fee_sum = base_fee_sum - data_table.base_fees
                  FROM
                    (SELECT
                      UNNEST(${sql.array(
                        addresses,
                      )}::text[]) as contract_address,
                      UNNEST(${sql.array(baseFees)}::float[]) as base_fees
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

let addedRowsLog: AddedBaseFeesLog[] = [];

export const addBlock = (
  blockNumber: number,
  baseFeeSums: ContractBaseFees,
): T.Task<void> =>
  pipe(
    getNewestIncludedBlockNumber(),
    T.map(
      O.getOrElseW(() => {
        throw new Error(
          "tried to add a block to empty leaderboard all, load all blocks first",
        );
      }),
    ),
    T.chain((newestIncludedBlock) =>
      pipe(
        blockNumber <= newestIncludedBlock,
        B.match(
          // no rollback
          () =>
            pipe(
              seqTPar(
                addContractBaseFeeSums(baseFeeSums),
                setNewestIncludedBlockNumber(blockNumber),
              ),
              T.map(() => undefined),
            ),
          // should rollback first!
          () => {
            Log.info(
              `rollback - newest included: ${newestIncludedBlock}, block number: ${blockNumber}`,
            );

            const sumsToRollback = Leaderboards.getPreviouslyAddedSums(
              addedRowsLog,
              blockNumber,
              newestIncludedBlock,
            );
            const numberOfBlocksToRollback =
              newestIncludedBlock - blockNumber + 1;

            return pipe(
              removeContractBaseFeeSums(sumsToRollback),
              T.chainIOK(() => () => {
                addedRowsLog = addedRowsLog.slice(0, -numberOfBlocksToRollback);
              }),
              T.chain(() =>
                seqTSeq(
                  addContractBaseFeeSums(baseFeeSums),
                  setNewestIncludedBlockNumber(blockNumber),
                ),
              ),
              T.map(() => undefined),
            );
          },
        ),
      ),
    ),
  );

type NoBlocks = { _tag: "no-blocks" };

export const addMissingBlocks = (): TE.TaskEither<NoBlocks, void> =>
  pipe(
    seqTPar(
      Blocks.getLatestStoredBlockNumber(),
      getNewestIncludedBlockNumber(),
    ),
    T.map(([latestStoredBlock, newestIncludedBlockO]) => {
      if (O.isNone(latestStoredBlock)) {
        return E.left({ _tag: "no-blocks" } as NoBlocks);
      }

      const newestIncludedBlock = pipe(
        newestIncludedBlockO,
        O.getOrElse(() => {
          Log.info(
            "no newest included block for leaderboard 'all', assuming fresh start",
          );
          return Blocks.londonHardForkBlockNumber - 1;
        }),
      );

      return E.right([latestStoredBlock.value, newestIncludedBlock] as [
        number,
        number,
      ]);
    }),
    TE.chain(([latestStoredBlock, newestIncludedBlock]) => {
      return pipe(
        Leaderboards.getRangeBaseFees(newestIncludedBlock, latestStoredBlock),
        T.chain((baseFeeSums) =>
          seqTPar(
            addContractBaseFeeSums(baseFeeSums),
            setNewestIncludedBlockNumber(latestStoredBlock),
          ),
        ),
        T.map(() => E.right(undefined)),
      );
    }),
  );

const getTopBaseFeeContracts = (): T.Task<LeaderboardRow[]> => {
  return () => sql<LeaderboardRow[]>`
    WITH top_base_fee_contracts AS (
      SELECT contract_address, base_fee_sum FROM contract_base_fee_sums
      ORDER BY (base_fee_sum) DESC
      LIMIT 24
    )
    SELECT contract_address, base_fee_sum AS base_fees, name, is_bot FROM top_base_fee_contracts
    JOIN contracts ON address = contract_address
  `;
};

export const calcLeaderboardAll = (): T.Task<LeaderboardEntry[]> => {
  return pipe(
    seqTPar(
      () => Leaderboards.getEthTransferFeesForTimeframe("all"),
      () => Leaderboards.getContractCreationBaseFeesForTimeframe("all"),
      getTopBaseFeeContracts(),
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
