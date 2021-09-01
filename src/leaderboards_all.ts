import * as Blocks from "./blocks.js";
import * as E from "fp-ts/lib/Either.js";
import * as Leaderboards from "./leaderboards.js";
import * as Log from "./log.js";
import * as O from "fp-ts/lib/Option.js";
import * as T from "fp-ts/lib/Task.js";
import * as TE from "fp-ts/lib/TaskEither.js";
import { LeaderboardEntry, LeaderboardRow } from "./leaderboards.js";
import { pipe } from "fp-ts/lib/function.js";
import { seqTPar } from "./sequence.js";
import { sql } from "./db.js";
import { A } from "./fp.js";

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

const setNewestIncludedBlockNumber = (blockNumber: number): T.Task<void> => {
  return pipe(
    () => sql`
      INSERT INTO base_fee_sum_included_blocks (oldest_included_block, newest_included_block, timeframe)
      VALUES (${Blocks.londonHardForkBlockNumber}, ${blockNumber}, 'all')
      ON CONFLICT (timeframe) DO UPDATE SET
        oldest_included_block = ${Blocks.londonHardForkBlockNumber},
        newest_included_block = ${blockNumber}
    `,
    T.map(() => undefined),
  );
};

type ContractBaseFeesRow = {
  contractAddress: string;
  baseFees: number;
};

const addBlocks = (from: number, upToIncluding: number): T.Task<void> =>
  pipe(
    () =>
      sql<ContractBaseFeesRow[]>`
        SELECT contract_address, SUM(base_fees) AS base_fees
        FROM contract_base_fees
        WHERE block_number >= ${from}
        AND block_number <= ${upToIncluding}
        GROUP BY (contract_address)
      `,
    T.map((rows) => (rows.length === 0 ? O.none : O.some(rows))),
    T.chain(
      O.match(
        () => T.of(undefined),
        (rows) =>
          pipe(
            rows,
            A.chunksOf(20000),
            A.map((chunk) => [
              chunk.map((row) => row.contractAddress),
              chunk.map((row) => row.baseFees),
            ]),
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
            T.chain(() => getNewestIncludedBlockNumber()),
            T.chain(
              O.match(
                () => setNewestIncludedBlockNumber(upToIncluding),
                (newestIncludedBlock) =>
                  upToIncluding > newestIncludedBlock
                    ? setNewestIncludedBlockNumber(upToIncluding)
                    : T.of(undefined),
              ),
            ),
          ),
      ),
    ),
    T.map(() => undefined),
  );
export const addBlock = (blockNumber: number): T.Task<void> =>
  addBlocks(blockNumber, blockNumber);

type NoBlocks = { _tag: "no-blocks" };

export const addMissingBlocks = (): TE.TaskEither<NoBlocks, void> =>
  pipe(
    seqTPar(
      Blocks.getLatestStoredBlockNumber(),
      getNewestIncludedBlockNumber(),
    ),
    T.map(([latestStoredBlock, newestIncludedBlock]) => {
      if (O.isNone(latestStoredBlock)) {
        return E.left({ _tag: "no-blocks" } as NoBlocks);
      }

      return E.right([latestStoredBlock.value, newestIncludedBlock] as [
        number,
        O.Option<number>,
      ]);
    }),
    TE.chain(
      ([latestStoredBlock, newestIncludedBlockO]): TE.TaskEither<
        NoBlocks,
        void
      > => {
        let newestIncludedBlock: number;
        if (O.isNone(newestIncludedBlockO)) {
          Log.info(
            "no newest included block for leaderboard 'all', assuming fresh start",
          );
          newestIncludedBlock = Blocks.londonHardForkBlockNumber - 1;
        } else {
          newestIncludedBlock = newestIncludedBlockO.value;
        }

        return pipe(
          addBlocks(newestIncludedBlock, latestStoredBlock),
          T.map(() => E.right(undefined)),
        );
      },
    ),
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
