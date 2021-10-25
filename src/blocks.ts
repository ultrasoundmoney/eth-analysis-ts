import * as A from "fp-ts/lib/Array.js";
import * as B from "fp-ts/lib/boolean.js";
import * as BaseFees from "./base_fees.js";
import * as BurnRates from "./burn_rates.js";
import * as Contracts from "./contracts.js";
import * as DateFns from "date-fns";
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
import { config } from "./config.js";
import PQueue from "p-queue";
import { BlockLondon } from "./eth_node.js";
import { FeeBreakdown } from "./base_fees.js";
import { LeaderboardEntries } from "./leaderboards.js";
import { Num, pipe, seqSParT, seqTParT, seqTSeqT } from "./fp.js";
import { TxRWeb3London } from "./transactions.js";
import { delay } from "./delay.js";
import { hexToNumber } from "./hexadecimal.js";
import { logPerfT } from "./performance.js";
import { performance } from "perf_hooks";
import { sql } from "./db.js";

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
  mined_at: DateFns.fromUnixTime(block.timestamp),
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

const getNewContractsFromBlock = (txrs: TxRWeb3London[]): string[] =>
  pipe(
    txrs,
    Transactions.segmentTxrs,
    (segments) => segments.contractCreationTxrs,
    A.map((txr) => txr.contractAddress),
    A.map(O.fromNullable),
    A.compact,
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

  const updateContractBaseFeesTask = seqTSeqT(
    () =>
      sql`DELETE FROM contract_base_fees WHERE block_number = ${block.number}`,
    contractBaseFeesRows.length !== 0
      ? () => sql`INSERT INTO contract_base_fees ${sql(contractBaseFeesRows)}`
      : T.of(undefined),
  );

  const storeContractsTask = Contracts.storeContracts(addresses);

  const updateContractsMinedAtTask = pipe(
    getNewContractsFromBlock(txrs),
    (addresses) =>
      Contracts.setContractsMinedAt(
        addresses,
        block.number,
        DateFns.fromUnixTime(block.timestamp),
      ),
  );

  return pipe(
    seqTSeqT(
      seqTParT(storeContractsTask, updateBlockTask),
      seqTParT(updateContractBaseFeesTask, updateContractsMinedAtTask),
    ),
    T.map(() => undefined),
  );
};

export const storeBlock = (
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

  const updateContractsMinedAtTask = pipe(
    getNewContractsFromBlock(txrs),
    (addresses) =>
      Contracts.setContractsMinedAt(
        addresses,
        block.number,
        DateFns.fromUnixTime(block.timestamp),
      ),
  );

  return pipe(
    seqTSeqT(
      seqTParT(storeContractsTask, storeBlockTask),
      seqTParT(storeContractsBaseFeesTask, updateContractsMinedAtTask),
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
  Log.debug("updating derived stats");
  const t0 = performance.now();
  const feesBurned = pipe(
    FeesBurned.calcFeesBurned(block),
    T.chainFirstIOK(logPerfT("calc fees burned", t0)),
  );
  const burnRates = pipe(
    BurnRates.calcBurnRates(block),
    T.chainFirstIOK(logPerfT("calc burn rates", t0)),
  );
  const leaderboardAll = pipe(
    LeaderboardsAll.calcLeaderboardAll(),
    T.chainFirstIOK(logPerfT("calc leaderboard all", t0)),
  );
  const leaderboardLimitedTimeframes = pipe(
    LeaderboardsLimitedTimeframe.calcLeaderboardForLimitedTimeframes(),
    T.chainFirstIOK(logPerfT("calc leaderboard limited timeframes", t0)),
  );
  const leaderboards: T.Task<LeaderboardEntries> = pipe(
    seqTParT(leaderboardLimitedTimeframes, leaderboardAll),
    T.map(([leaderboardLimitedTimeframes, leaderboardAll]) => ({
      leaderboard5m: leaderboardLimitedTimeframes["5m"],
      leaderboard1h: leaderboardLimitedTimeframes["1h"],
      leaderboard24h: leaderboardLimitedTimeframes["24h"],
      leaderboard7d: leaderboardLimitedTimeframes["7d"],
      leaderboard30d: leaderboardLimitedTimeframes["30d"],
      leaderboardAll: leaderboardAll,
    })),
  );

  return pipe(
    seqSParT({ burnRates, feesBurned, leaderboards }),
    T.chain(({ burnRates, feesBurned, leaderboards }) =>
      DerivedBlockStats.storeDerivedBlockStats({
        blockNumber: block.number,
        burnRates,
        feesBurned,
        leaderboards,
      }),
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

export const addMissingBlocks = async (upToIncluding: number) => {
  Log.debug("checking for missing blocks");
  const wantedBlockRange = getBlockRange(
    londonHardForkBlockNumber,
    upToIncluding,
  );

  const storedBlocks = await getKnownBlocks()();
  const missingBlocks = wantedBlockRange.filter(
    (wantedBlockNumber) => !storedBlocks.has(wantedBlockNumber),
  );

  if (missingBlocks.length !== 0) {
    Log.info("blocks table out-of-sync");

    Log.info(`adding ${missingBlocks.length} missing blocks`);

    if (process.env.SHOW_PROGRESS !== undefined) {
      DisplayProgress.start(missingBlocks.length);
    }

    await storeBlockQueuePar.addAll(
      missingBlocks.map((blockNumber) =>
        pipe(
          () => getBlockWithRetry(blockNumber),
          T.chain((block) =>
            seqTParT(T.of(block), () => Transactions.getTxrsWithRetry(block)),
          ),
          T.chain(([block, txrs]) => storeBlock(block, txrs)),
        ),
      ),
    );
    Log.info(`added ${missingBlocks.length} missing blocks`);
  }

  PerformanceMetrics.setShouldLogBlockFetchRate(false);
};

export const storeNewBlock = (blockNumber: number): T.Task<void> =>
  pipe(
    getLastNKnownBlocks(20),
    T.chain((knownBlocks2) => {
      return pipe(
        () => getBlockWithRetry(blockNumber),
        T.chainFirstIOK(() => () => {
          Log.debug(`analyzing block ${blockNumber}`);
        }),
        // Rollback leadersboards if needed.
        T.chainFirst((block) => {
          if (!knownBlocks2.has(block.number)) {
            return T.of(undefined);
          }
          const t0 = performance.now();
          const blocksToRollback = pipe(
            knownBlocks2.values(),
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
            T.chainFirstIOK(logPerfT("rollback", t0)),
          );
        }),
        T.chain((block) =>
          seqTParT(T.of(block), () => Transactions.getTxrsWithRetry(block)),
        ),
        T.chainFirstIOK(() => () => {
          if (config.showProgress) {
            DisplayProgress.onBlockAnalyzed();
          }
        }),
        T.chain(([block, txrs]) =>
          pipe(
            knownBlocks2.has(block.number),
            B.match(
              () => storeBlock(block, txrs),
              () => updateBlock(block, txrs),
            ),
            T.chain(() => {
              const contractBaseFees = pipe(
                BaseFees.calcBlockFeeBreakdown(block, txrs),
                (feeBreakdown) => feeBreakdown.contract_use_fees,
                (useFees) => Object.entries(useFees),
                (entries) => new Map(entries),
              );

              const t0 = performance.now();

              return pipe(
                seqTParT(
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
                ),
                T.chainFirstIOK(logPerfT("adding block to leaderboards", t0)),
                T.chainFirstIOK(() => () => PerformanceMetrics.logQueueSizes()),
              );
            }),
            T.chain(() => {
              Log.debug(`store block seq queue ${storeBlockQueueSeq.size}`);
              Log.debug(
                `add leaderboard all queue ${addLeaderboardAllQueue.size}`,
              );
              Log.debug(
                `add leaderboard limited timeframe queue: ${addLeaderboardLimitedTimeframeQueue.size}`,
              );
              const allBlocksProcessed =
                storeBlockQueuePar.size === 0 &&
                storeBlockQueuePar.pending === 0 &&
                storeBlockQueueSeq.size < 8 &&
                // This function is on this queue.
                storeBlockQueueSeq.pending < 8 &&
                addLeaderboardAllQueue.size < 8 &&
                addLeaderboardAllQueue.pending < 8 &&
                addLeaderboardLimitedTimeframeQueue.size < 8 &&
                addLeaderboardLimitedTimeframeQueue.pending < 8;
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
    }),
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

export const getLastNKnownBlocks = (count: number): T.Task<Set<number>> =>
  pipe(
    () => sql<{ number: number }[]>`
      SELECT number
      FROM blocks
      ORDER BY number DESC
      LIMIT ${count}
    `,
    T.map((rows) =>
      pipe(
        rows,
        A.map((row) => row.number),
        (numbers) => new Set(numbers),
      ),
    ),
  );
