import * as A from "fp-ts/lib/Array.js";
import * as B from "fp-ts/lib/boolean.js";
import * as BaseFees from "./base_fees.js";
import * as BurnRates from "./burn_rates.js";
import * as Contracts from "./contracts.js";
import * as DerivedBlockStats from "./derived_block_stats.js";
import * as DisplayProgress from "./display_progress.js";
import * as Duration from "./duration.js";
import * as EthNode from "./eth_node.js";
import * as FeesBurned from "./fees_burned.js";
import * as Leaderboards from "./leaderboards.js";
import * as LeaderboardsAll from "./leaderboards_all.js";
import * as LeaderboardsLimitedTimeframe from "./leaderboards_limited_timeframe.js";
import * as Log from "./log.js";
import * as O from "fp-ts/lib/Option.js";
import * as PerformanceMetrics from "./performance_metrics.js";
import * as Sentry from "@sentry/node";
import * as T from "fp-ts/lib/Task.js";
import * as Transactions from "./transactions.js";
import Config from "./config.js";
import { BlockLondon } from "./eth_node.js";
import { FeeBreakdown } from "./base_fees.js";
import { TxRWeb3London } from "./transactions.js";
import { delay } from "./delay.js";
import { fromUnixTime } from "date-fns";
import { hexToNumber } from "./hexadecimal.js";
import { pipe } from "fp-ts/lib/function.js";
import { seqSPar, seqTPar, seqTSeq } from "./sequence.js";
import { sql } from "./db.js";
import { LeaderboardEntries } from "./leaderboards.js";
import PQueue from "p-queue";
import { Num } from "./fp.js";

export const londonHardForkBlockNumber = 12965000;

export const getBlockRange = (from: number, toAndIncluding: number): number[] =>
  new Array(toAndIncluding - from + 1)
    .fill(undefined)
    .map((_, i) => toAndIncluding - i)
    .reverse();

export const getBlockWithRetry = async (
  blockNumber: number | "latest" | string,
): Promise<BlockLondon> => {
  const delayMilis = Duration.milisFromSeconds(3);
  const delaySeconds = delayMilis * 1000;
  let tries = 0;

  // Retry continuously
  // eslint-disable-next-line no-constant-condition
  while (true) {
    tries += tries + 1;

    const maybeBlock = await EthNode.getBlock(blockNumber);

    if (typeof maybeBlock?.hash === "string") {
      PerformanceMetrics.onBlockReceived();
      return maybeBlock;
    }

    if (tries === 10) {
      Sentry.captureException(
        new Error(
          `stuck fetching block, for more than ${tries * delaySeconds}s`,
        ),
      );
    }

    if (tries > 20) {
      throw new Error("failed to fetch block, stayed null");
    }

    Log.warn(
      `asked for block ${blockNumber}, got null, waiting ${delaySeconds}s and trying again`,
    );
    await delay(delayMilis);
  }
};

export const getLatestStoredBlockNumber = (): T.Task<O.Option<number>> => () =>
  sql`
    SELECT MAX(number) AS number FROM blocks
  `.then((result) => pipe(result[0]?.number, O.fromNullable));

export const storeBlockQueuePar = new PQueue({ concurrency: 8 });
export const storeBlockQueueSeq = new PQueue({ concurrency: 1 });

type BlockRow = {
  hash: string;
  number: number;
  base_fees: unknown;
  mined_at: Date;
  tips: number;
  base_fee_sum: number;
  contract_creation_sum: number;
  eth_transfer_sum: number;
  base_fee_per_gas: number;
  gas_used: number;
};

const getBlockRow = (
  block: BlockLondon,
  feeBreakdown: FeeBreakdown,
  tips: number,
): BlockRow => ({
  hash: block.hash,
  number: block.number,
  base_fees: sql.json(feeBreakdown),
  mined_at: fromUnixTime(block.timestamp),
  tips: tips,
  base_fee_sum: BaseFees.calcBlockBaseFeeSum(block),
  contract_creation_sum: feeBreakdown.contract_creation_fees,
  eth_transfer_sum: feeBreakdown.transfers,
  base_fee_per_gas: hexToNumber(block.baseFeePerGas),
  gas_used: block.gasUsed,
});

type ContractBaseFeesRow = {
  contract_address: string;
  base_fees: number;
  block_number: number;
};

const getContractRows = (
  block: BlockLondon,
  feeBreakdown: FeeBreakdown,
): ContractBaseFeesRow[] =>
  pipe(
    feeBreakdown.contract_use_fees,
    Object.entries,
    A.map(([address, baseFees]) => ({
      base_fees: baseFees,
      block_number: block.number,
      contract_address: address,
    })),
  );

export const updateBlock = (
  block: BlockLondon,
  txrs: TxRWeb3London[],
): T.Task<void> => {
  const feeBreakdown = BaseFees.calcBlockFeeBreakdown(block, txrs);
  const tips = BaseFees.calcBlockTips(block, txrs);
  const blockRow = getBlockRow(block, feeBreakdown, tips);
  const contractBaseFeesRows = getContractRows(block, feeBreakdown);

  const addresses = contractBaseFeesRows.map(
    (contractBurnRow) => contractBurnRow.contract_address,
  );

  const updateBlockTask = () =>
    sql`
      UPDATE blocks
      SET
        ${sql(blockRow)}
      WHERE
        number = ${block.number}
    `.then(() => undefined);

  const updateContractBaseFeesTask = seqTSeq(
    () =>
      sql`DELETE FROM contract_base_fees WHERE block_number = ${block.number}`,
    contractBaseFeesRows.length !== 0
      ? () => sql`INSERT INTO contract_base_fees ${sql(contractBaseFeesRows)}`
      : T.of(undefined),
  );

  const storeContractsTask = Contracts.storeContracts(addresses);

  return pipe(
    seqTSeq(
      seqTPar(storeContractsTask, updateBlockTask),
      updateContractBaseFeesTask,
    ),
    T.map(() => undefined),
  );
};

const storeBlock = (
  block: BlockLondon,
  txrs: TxRWeb3London[],
): T.Task<void> => {
  const feeBreakdown = BaseFees.calcBlockFeeBreakdown(block, txrs);
  const tips = BaseFees.calcBlockTips(block, txrs);
  const blockRow = getBlockRow(block, feeBreakdown, tips);
  const contractBaseFeesRows = getContractRows(block, feeBreakdown);

  const addresses = contractBaseFeesRows.map(
    (contractBurnRow) => contractBurnRow.contract_address,
  );

  const storeBlockTask = () => sql`INSERT INTO blocks ${sql(blockRow)}`;

  const storeContractsTask = Contracts.storeContracts(addresses);

  const storeContractsBaseFeesTask =
    contractBaseFeesRows.length !== 0
      ? () => sql`INSERT INTO contract_base_fees ${sql(contractBaseFeesRows)}`
      : T.of(undefined);

  return pipe(
    seqTSeq(
      seqTPar(storeContractsTask, storeBlockTask),
      storeContractsBaseFeesTask,
    ),
    T.map(() => undefined),
  );
};

export const getIsKnownBlock = (block: BlockLondon): T.Task<boolean> =>
  pipe(
    () =>
      sql<
        { isKnown: boolean }[]
      >`SELECT EXISTS(SELECT number FROM blocks WHERE number = ${block.number}) AS is_known`,
    T.map((rows) => rows[0]?.isKnown ?? false),
  );

export type NewBlockPayload = {
  number: number;
};

const notifyNewDerivedStats = (block: BlockLondon): T.Task<void> => {
  const payload: NewBlockPayload = {
    number: block.number,
  };

  return pipe(
    () => sql.notify("new-derived-stats", JSON.stringify(payload)),
    T.map(() => undefined),
  );
};

const updateDerivedBlockStats = (block: BlockLondon) => {
  const feesBurned = FeesBurned.calcFeesBurned(block);
  const burnRates = BurnRates.calcBurnRates(block);
  const leaderboardAll = LeaderboardsAll.calcLeaderboardAll();
  const leaderboardLimitedTimeframes =
    LeaderboardsLimitedTimeframe.calcLeaderboardForLimitedTimeframes();
  const leaderboards: T.Task<LeaderboardEntries> = pipe(
    seqTPar(leaderboardLimitedTimeframes, leaderboardAll),
    T.map(([leaderboardLimitedTimeframes, leaderboardAll]) => ({
      leaderboard1h: leaderboardLimitedTimeframes["1h"],
      leaderboard24h: leaderboardLimitedTimeframes["24h"],
      leaderboard7d: leaderboardLimitedTimeframes["7d"],
      leaderboard30d: leaderboardLimitedTimeframes["30d"],
      leaderboardAll: leaderboardAll,
    })),
  );

  return pipe(
    seqSPar({ burnRates, feesBurned, leaderboards }),
    T.chain((derivedBlockStats) =>
      DerivedBlockStats.storeDerivedBlockStats(block, derivedBlockStats),
    ),
  );
};

// Removing blocks in parallel is problematic. Make sure to do so one by one.
const removeBlocksQueue = new PQueue({ concurrency: 1 });

// Adding blocks in parallel is problematic. Make sure to do so one by one.
export const addLeaderboardAllQueue = new PQueue({
  autoStart: false,
  concurrency: 1,
});
export const addLeaderboardLimitedTimeframeQueue = new PQueue({
  autoStart: false,
  concurrency: 1,
});

export const storeNewBlock = (
  knownBlocks: Set<number>,
  blockNumber: number,
): T.Task<void> =>
  pipe(
    () => getBlockWithRetry(blockNumber),
    T.chainFirstIOK(() => () => {
      Log.debug(`analyzing block ${blockNumber}`);
    }),
    // Rollback leadersboards if needed.
    T.chainFirst((block) => {
      if (!knownBlocks.has(block.number)) {
        return T.of(undefined);
      }
      const blocksToRollback = pipe(
        knownBlocks.values(),
        (blocksIter) => Array.from(blocksIter),
        A.filter((knownBlockNumber) => knownBlockNumber >= block.number),
        A.sort(Num.Ord),
      );

      return pipe(
        Leaderboards.getRangeBaseFees(
          blocksToRollback[0],
          blocksToRollback[blocksToRollback.length - 1],
        ),
        T.chain((sumsToRollback) => {
          LeaderboardsLimitedTimeframe.rollbackToBefore(
            block.number,
            sumsToRollback,
          );
          return LeaderboardsAll.removeContractBaseFeeSums(sumsToRollback);
        }),
      );
    }),
    T.chain((block) =>
      seqTPar(T.of(block), () => Transactions.getTxrsWithRetry(block)),
    ),
    T.chainFirstIOK(() => () => {
      if (Config.showProgress) {
        DisplayProgress.onBlockAnalyzed();
      }
    }),
    T.chain(([block, txrs]) =>
      pipe(
        knownBlocks.has(block.number),
        B.match(
          () => storeBlock(block, txrs),
          () => updateBlock(block, txrs),
        ),
        T.chain(() => {
          knownBlocks.add(block.number);
          const contractBaseFees = pipe(
            BaseFees.calcBlockFeeBreakdown(block, txrs),
            (feeBreakdown) => feeBreakdown.contract_use_fees,
            (useFees) => Object.entries(useFees),
            (entries) => new Map(entries),
          );

          return seqTPar(
            () =>
              addLeaderboardLimitedTimeframeQueue.add(() =>
                LeaderboardsLimitedTimeframe.addBlockForAllTimeframes(
                  block,
                  contractBaseFees,
                ),
              ),
            () =>
              removeBlocksQueue.add(
                LeaderboardsLimitedTimeframe.removeExpiredBlocksFromSumsForAllTimeframes(),
              ),
            () =>
              addLeaderboardAllQueue.add(
                LeaderboardsAll.addBlock(block.number, contractBaseFees),
              ),
          );
        }),
        T.chain(() => {
          const allBlocksProcessed =
            storeBlockQueuePar.size === 0 &&
            storeBlockQueuePar.pending === 0 &&
            storeBlockQueueSeq.size === 0 &&
            // This function is on this queue.
            storeBlockQueueSeq.pending <= 1 &&
            addLeaderboardAllQueue.size === 0 &&
            addLeaderboardAllQueue.pending === 0 &&
            addLeaderboardLimitedTimeframeQueue.size === 0 &&
            addLeaderboardLimitedTimeframeQueue.pending === 0;
          if (allBlocksProcessed) {
            return pipe(
              updateDerivedBlockStats(block),
              T.chain(() => notifyNewDerivedStats(block)),
            );
          } else {
            Log.debug(
              "blocks left to process, skipping computation of derived stats",
            );
            return T.of(undefined);
          }
        }),
      ),
    ),
  );

export const getKnownBlocks = (): T.Task<Set<number>> =>
  pipe(
    () => sql<{ number: number }[]>`SELECT number FROM blocks`,
    T.map((rows) =>
      pipe(
        rows,
        A.map((row) => row.number),
        (numbers) => new Set(numbers),
      ),
    ),
  );
