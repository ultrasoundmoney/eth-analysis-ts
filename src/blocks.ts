import * as Sentry from "@sentry/node";
import * as DateFns from "date-fns";
import PQueue from "p-queue";
import { performance } from "perf_hooks";
import * as BaseFees from "./base_fees.js";
import { FeeBreakdown } from "./base_fees.js";
import * as BaseFeeSums from "./base_fee_sums.js";
import * as BurnRates from "./burn_rates.js";
import * as Config from "./config.js";
import * as Contracts from "./contracts.js";
import { sql } from "./db.js";
import { delay } from "./delay.js";
import * as DerivedBlockStats from "./derived_block_stats.js";
import * as DisplayProgress from "./display_progress.js";
import * as Duration from "./duration.js";
import * as EthNode from "./eth_node.js";
import { BlockLondon } from "./eth_node.js";
import * as EthPrices from "./eth_prices.js";
import { A, B, Num, O, pipe, seqSParT, seqTParT, seqTSeqT, T } from "./fp.js";
import { hexToNumber } from "./hexadecimal.js";
import * as Leaderboards from "./leaderboards.js";
import { LeaderboardEntries } from "./leaderboards.js";
import * as LeaderboardsAll from "./leaderboards_all.js";
import * as LeaderboardsLimitedTimeframe from "./leaderboards_limited_timeframe.js";
import * as Log from "./log.js";
import { logPerfT } from "./performance.js";
import * as PerformanceMetrics from "./performance_metrics.js";
import * as Transactions from "./transactions.js";
import { TxRWeb3London } from "./transactions.js";

export const londonHardForkBlockNumber = 12965000;

export const getBlockRange = (from: number, toAndIncluding: number): number[] =>
  new Array(toAndIncluding - from + 1)
    .fill(undefined)
    .map((_, i) => toAndIncluding - i)
    .reverse();

export const getBlockWithRetry = async (
  blockNumber: number | "latest" | string,
): Promise<BlockLondon> => {
  const delayMilis = Duration.millisFromSeconds(3);
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

export const getLatestStoredBlockNumber = (): T.Task<O.Option<number>> =>
  pipe(
    () => sql<{ number: number }[]>`
      SELECT MAX(number) AS number FROM blocks
    `,
    T.map((rows) => rows[0]?.number),
    T.map(O.fromNullable),
  );

export const storeMissingBlockQueue = new PQueue({ concurrency: 1 });
export const storeNewBlockQueue = new PQueue({ concurrency: 1 });

export type BlockRow = {
  hash: string;
  number: number;
  mined_at: Date;
  tips: number;
  base_fee_sum: number;
  base_fee_sum_256: bigint;
  contract_creation_sum: number;
  eth_transfer_sum: number;
  base_fee_per_gas: number;
  gas_used: number;
  eth_price?: number;
};

export type BlockDb = {
  baseFeePerGas: number;
  baseFeeSum: bigint;
  contractCreationSum: number;
  ethPrice: number;
  ethTransferSum: number;
  gasUsed: number;
  hash: string;
  minedAt: Date;
  number: number;
  tips: number;
};

const getBlockRow = (
  block: BlockLondon,
  feeBreakdown: FeeBreakdown,
  tips: number,
  ethPrice: number | undefined,
): BlockRow => ({
  hash: block.hash,
  number: block.number,
  mined_at: DateFns.fromUnixTime(block.timestamp),
  tips: tips,
  base_fee_sum: Number(BaseFees.calcBlockBaseFeeSum(block)),
  base_fee_sum_256: BaseFees.calcBlockBaseFeeSum(block),
  contract_creation_sum: feeBreakdown.contract_creation_fees,
  eth_transfer_sum: feeBreakdown.transfers,
  base_fee_per_gas: hexToNumber(block.baseFeePerGas),
  gas_used: block.gasUsed,
  ...(typeof ethPrice === "number" ? { eth_price: ethPrice } : undefined),
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
    Array.from(feeBreakdown.contract_use_fees.entries()),
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

const getBlockHashIsKnown = (hash: string): T.Task<boolean> =>
  pipe(
    () => sql<{ isKnown: boolean }[]>`
      SELECT EXISTS(SELECT hash FROM blocks WHERE hash = ${hash}) AS is_known
    `,
    T.map((rows) => rows[0]?.isKnown === true ?? false),
  );

export const updateBlock = (
  block: BlockLondon,
  txrs: TxRWeb3London[],
  ethPrice?: number,
): T.Task<void> => {
  const feeBreakdown = BaseFees.calcBlockFeeBreakdown(block, txrs);
  const tips = BaseFees.calcBlockTips(block, txrs);
  const blockRow = getBlockRow(block, feeBreakdown, tips, ethPrice);
  const contractBaseFeesRows = getContractRows(block, feeBreakdown);

  const addresses = contractBaseFeesRows.map(
    (contractBurnRow) => contractBurnRow.contract_address,
  );

  const updateBlockTask = pipe(
    () => sql`
      UPDATE blocks
      SET
        ${sql(blockRow)}
      WHERE
        number = ${block.number}
    `,
    T.map(() => undefined),
  );

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
  ethPrice: number | undefined,
): T.Task<void> => {
  const feeBreakdown = BaseFees.calcBlockFeeBreakdown(block, txrs);
  const tips = BaseFees.calcBlockTips(block, txrs);
  const contractBaseFeesRows = getContractRows(block, feeBreakdown);
  const blockRow = getBlockRow(block, feeBreakdown, tips, ethPrice);

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

const getIsKnownBlock = (blockNumber: number): T.Task<boolean> =>
  pipe(
    () =>
      sql<
        { isKnown: boolean }[]
      >`SELECT EXISTS(SELECT number FROM blocks WHERE number = ${blockNumber}) AS is_known`,
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
    BaseFeeSums.calcBaseFeeSums(block),
    T.chainFirstIOK(logPerfT("calc base fee sums", t0)),
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
    T.chainFirstIOK(() => () => {
      DerivedBlockStats.deleteOldDerivedStats()();
    }),
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

export const addMissingBlocks = async (
  upToIncluding: number,
): Promise<void> => {
  Log.debug("checking for missing blocks");
  const wantedBlockRange = getBlockRange(
    londonHardForkBlockNumber,
    upToIncluding,
  );

  const storedBlocks = await getKnownBlocks()();
  const missingBlocks = wantedBlockRange.filter(
    (wantedBlockNumber) => !storedBlocks.has(wantedBlockNumber),
  );

  if (missingBlocks.length === 0) {
    PerformanceMetrics.setShouldLogBlockFetchRate(false);
    return undefined;
  }

  Log.info("blocks table out-of-sync");

  Log.info(`adding ${missingBlocks.length} missing blocks`);

  if (process.env.SHOW_PROGRESS !== undefined) {
    DisplayProgress.start(missingBlocks.length);
  }

  await storeMissingBlockQueue.addAll(
    missingBlocks.map((blockNumber) =>
      pipe(
        () => getBlockWithRetry(blockNumber),
        T.chain((block) =>
          seqTParT(
            T.of(block),
            () => Transactions.getTxrsWithRetry(block),
            EthPrices.getEthPrice(DateFns.fromUnixTime(block.timestamp)),
          ),
        ),
        T.chain(([block, txrs, ethPrice]) =>
          storeBlock(block, txrs, ethPrice?.ethusd),
        ),
      ),
    ),
  );

  missingBlocks.forEach((blockNumber) => knownBlocks.add(blockNumber));

  Log.info(`added ${missingBlocks.length} missing blocks`);
  PerformanceMetrics.setShouldLogBlockFetchRate(false);
};

const knownBlocks: Set<number> = new Set();

const rollback = (block: BlockLondon): T.Task<void> => {
  const t0 = performance.now();

  Log.info(`rolling back block: ${block.number}`);

  return pipe(
    Leaderboards.getRangeBaseFees(block.number, block.number),
    T.chain((sumsToRollback) => {
      LeaderboardsLimitedTimeframe.rollbackToBefore(
        block.number,
        sumsToRollback,
      );
      return pipe(
        LeaderboardsAll.removeContractBaseFeeSums(sumsToRollback),
        T.map(() => undefined),
      );
    }),
    T.chainFirstIOK(logPerfT("rollback", t0)),
  );
};

export const storeNewBlock = (blockNumber: number): T.Task<void> =>
  pipe(
    Log.debug(`analyzing block ${blockNumber}`),
    () =>
      seqSParT({
        block: () => getBlockWithRetry(blockNumber),
        isKnownBlock: getIsKnownBlock(blockNumber),
      }),
    T.chain(({ block, isKnownBlock }) =>
      seqSParT({
        block: T.of(block),
        isKnownBlock: T.of(isKnownBlock),
        txrs: () => Transactions.getTxrsWithRetry(block),
        ethPrice: EthPrices.getEthPrice(DateFns.fromUnixTime(block.timestamp)),
      }),
    ),
    T.chainFirst(({ block, isKnownBlock, txrs, ethPrice }) =>
      pipe(
        isKnownBlock,
        B.match(
          () => storeBlock(block, txrs, ethPrice.ethusd),
          // Rollback
          () =>
            pipe(
              rollback(block),
              T.chain(() => updateBlock(block, txrs)),
            ),
        ),
      ),
    ),
    T.chainFirstIOK(() => () => {
      if (Config.getShowProgress()) {
        DisplayProgress.onBlockAnalyzed();
      }
    }),
    T.chainFirst(({ block, txrs, ethPrice }) => {
      const feeBreakdown = BaseFees.calcBlockFeeBreakdown(
        block,
        txrs,
        ethPrice.ethusd,
      );

      const t0 = performance.now();

      const addToLimitedTimeFrameTask = () =>
        addLeaderboardLimitedTimeframeQueue.add(() =>
          LeaderboardsLimitedTimeframe.addBlockForAllTimeframes(
            block,
            feeBreakdown.contract_use_fees,
            feeBreakdown.contract_use_fees_usd!,
          ),
        );

      const removeExpiredBlocksTask = () =>
        removeBlocksQueue.add(
          LeaderboardsLimitedTimeframe.removeExpiredBlocksFromSumsForAllTimeframes(),
        );

      const addToLeaderboardAllTask = () =>
        addLeaderboardAllQueue.add(
          LeaderboardsAll.addBlock(
            block.number,
            feeBreakdown.contract_use_fees,
            feeBreakdown.contract_use_fees_usd!,
          ),
        );

      return pipe(
        seqTParT(
          addToLimitedTimeFrameTask,
          removeExpiredBlocksTask,
          addToLeaderboardAllTask,
        ),
        T.chainFirstIOK(logPerfT("adding block to leaderboards", t0)),
      );
    }),
    T.chain(({ block }) => {
      Log.debug(`store block seq queue ${storeNewBlockQueue.size}`);
      const allBlocksProcessed =
        storeNewBlockQueue.size === 0 &&
        // This function is on this queue.
        storeNewBlockQueue.pending <= 1 &&
        addLeaderboardAllQueue.size === 0 &&
        addLeaderboardAllQueue.pending === 0 &&
        addLeaderboardLimitedTimeframeQueue.size === 0 &&
        addLeaderboardLimitedTimeframeQueue.pending === 0;

      if (!allBlocksProcessed) {
        Log.debug(
          "blocks left to process, skipping computation of derived stats",
        );
        return T.of(undefined);
      }

      return pipe(
        updateDerivedBlockStats(block),
        T.chain(() => notifyNewDerivedStats(block)),
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

export const getBaseFeesPerGas = (
  blockNumber: number,
): T.Task<number | undefined> => {
  return pipe(
    () => sql<{ baseFeePerGas: number }[]>`
      SELECT base_fee_per_gas FROM blocks
      WHERE number = ${blockNumber}
    `,
    T.map((rows) =>
      typeof rows[0]?.baseFeePerGas === "bigint"
        ? Number(rows[0].baseFeePerGas)
        : undefined,
    ),
  );
};

export const setEthPrice = (
  blockNumber: number,
  ethPrice: number,
): T.Task<void> =>
  pipe(
    () => sql`
      UPDATE blocks
      SET eth_price = ${ethPrice}
      WHERE number = ${blockNumber}
    `,
    T.map(() => undefined),
  );

export const getLatestBaseFeePerGas = (): T.Task<number> =>
  pipe(
    () => sql<{ baseFeePerGas: number }[]>`
      SELECT base_fee_per_gas FROM blocks
      ORDER BY number DESC
      LIMIT 1
    `,
    T.map((rows) =>
      typeof rows[0]?.baseFeePerGas === "bigint"
        ? Number(rows[0].baseFeePerGas)
        : 0,
    ),
  );
