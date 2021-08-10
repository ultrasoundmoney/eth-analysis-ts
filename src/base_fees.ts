import { sql } from "./db.js";
import type { TxRWeb3London } from "./transactions";
import A from "fp-ts/lib/Array.js";
import NEA from "fp-ts/lib/NonEmptyArray.js";
import R from "fp-ts/lib/Record.js";
import { pipe } from "fp-ts/lib/function.js";
import * as Log from "./log.js";
import { hexToNumber, sum, weiToEth } from "./numbers.js";
import { BlockLondon } from "./web3.js";
import neatCsv from "neat-csv";
import fs from "fs/promises";
import * as Transactions from "./transactions.js";
import * as eth from "./web3.js";
import * as DisplayProgress from "./display_progress.js";
import PQueue from "p-queue";
import * as ROA from "fp-ts/lib/ReadonlyArray.js";
import * as Blocks from "./blocks.js";
import Sentry from "@sentry/node";
import * as T from "fp-ts/lib/Task.js";
import { sequenceT, sequenceS } from "fp-ts/lib/Apply.js";

export type FeeBreakdown = {
  /** fees burned for simple transfers. */
  transfers: number;
  /** fees burned for use of contracts. */
  contract_use_fees: Partial<Record<string, number>>;
  /** fees burned for the creation of contracts. */
  contract_creation_fees: number;
};

export const getLatestAnalyzedBlockNumber = (): Promise<number | undefined> =>
  sql`
    SELECT MAX(number) AS number FROM base_fees_per_block
  `.then((result) => result[0]?.number || undefined);

const getBlockTimestamp = (block: BlockLondon): number => {
  // TODO: remove this if no errors are reported.
  if (typeof block.timestamp !== "number") {
    Log.error(
      `block ${block.number} had unexpected timestamp: ${block.timestamp}`,
    );
  }

  return block.timestamp;
};

const storeBaseFeesForBlock = async (
  block: BlockLondon,
  baseFees: FeeBreakdown,
  baseFeeSum: number,
  tips: number,
): Promise<void> =>
  sql`
  INSERT INTO base_fees_per_block
    (hash, number, base_fees, mined_at, tips, base_fee_sum)
  VALUES (
    ${block.hash},
    ${block.number},
    ${sql.json(baseFees)},
    to_timestamp(${getBlockTimestamp(block)}),
    ${tips},
    ${baseFeeSum}
  )
  ON CONFLICT (number) DO UPDATE
  SET
    hash = ${block.hash},
    number = ${block.number},
    base_fees = ${sql.json(baseFees)},
    mined_at = to_timestamp(${getBlockTimestamp(block)}),
    tips = ${tips},
    base_fee_sum = ${baseFeeSum}
  `.then(() => undefined);

export const calcTxrBaseFee = (
  block: BlockLondon,
  txr: TxRWeb3London,
): number => hexToNumber(block.baseFeePerGas) * txr.gasUsed;

/**
 * Map of base fees grouped by contract address
 */
type ContractBaseFeeMap = Record<string, number>;

const calcBaseFeePerContract = (
  block: BlockLondon,
  txrs: TxRWeb3London[],
): ContractBaseFeeMap =>
  pipe(
    txrs,
    A.reduce({} as ContractBaseFeeMap, (feeSumMap, txr: TxRWeb3London) => {
      // Contract creation
      if (txr.to === null) {
        return feeSumMap;
      }

      const baseFeeSum = feeSumMap[txr.to] || 0;
      feeSumMap[txr.to] = baseFeeSum + calcTxrBaseFee(block, txr);

      return feeSumMap;
    }),
  );

// Name is undefined because we don't always know the name for a contract. Image is undefined because we don't always have an image for a contract. Address is undefined because base fees paid for ETH transfers are shared between many addresses.
export type BaseFeeBurner = {
  name: string | undefined;
  image: string | undefined;
  fees: number;
  id: string;
};

export const sumFeeMaps = (
  maps: Partial<Record<string, number>>[],
): Record<string, number> =>
  (maps as Record<string, number>[]).reduce((sumMap, map) => {
    Object.entries(map as Record<string, number>).forEach(([key, num]) => {
      const sum = sumMap[key] || 0;
      sumMap[key] = sum + num;
    });
    return sumMap;
  }, {} as Record<string, number>);

export type Timeframe = "1h" | "24h" | "7d" | "30d" | "all";

let contractNameMap: Partial<Record<string, string>> | undefined = undefined;
export const getContractNameMap = async () => {
  if (contractNameMap !== undefined) {
    return contractNameMap;
  }

  const knownContracts = await neatCsv<{ dapp: string; address: string }>(
    await fs.readFile("./master_list.csv"),
  );

  contractNameMap = pipe(
    knownContracts,
    NEA.groupBy((knownContract) => knownContract.address),
    R.map((knownContractsForAddress) => knownContractsForAddress[0].dapp),
  );

  return contractNameMap;
};

export const calcBlockBaseFeeSum = (block: BlockLondon): number =>
  block.gasUsed * hexToNumber(block.baseFeePerGas);

export type FeesBurned = {
  feesBurned1h: number;
  feesBurned24h: number;
  feesBurned7d: number;
  feesBurned30d: number;
  feesBurnedAll: number;
};

export const getTotalFeesBurned = async (): Promise<FeesBurned> => {
  const feesBurned1h = () =>
    sql<{ baseFeeSum: number }[]>`
      SELECT SUM(base_fee_sum) as base_fee_sum FROM base_fees_per_block
      WHERE mined_at >= now() - interval '1 hours'
  `.then((rows) => rows[0]?.baseFeeSum ?? 0);

  const feesBurned24h = () =>
    sql<{ baseFeeSum: number }[]>`
      SELECT SUM(base_fee_sum) as base_fee_sum FROM base_fees_per_block
  `.then((rows) => rows[0]?.baseFeeSum ?? 0);

  const feesBurned7d = () =>
    sql<{ baseFeeSum: number }[]>`
      SELECT SUM(base_fee_sum) as base_fee_sum FROM base_fees_per_block
  `.then((rows) => rows[0]?.baseFeeSum ?? 0);

  const feesBurned30d = () =>
    sql<{ baseFeeSum: number }[]>`
      SELECT SUM(base_fee_sum) as base_fee_sum FROM base_fees_per_block
  `.then((rows) => rows[0]?.baseFeeSum ?? 0);

  const feesBurnedAll = () =>
    sql<{ baseFeeSum: number }[]>`
      SELECT SUM(base_fee_sum) as base_fee_sum FROM base_fees_per_block
  `.then((rows) => rows[0]?.baseFeeSum ?? 0);

  return sequenceS(T.ApplyPar)({
    feesBurned1h,
    feesBurned24h,
    feesBurned7d,
    feesBurned30d,
    feesBurnedAll,
  })();
};

export type FeesBurnedPerInterval = Record<string, number>;

export const getFeesBurnedPerInterval =
  async (): Promise<FeesBurnedPerInterval> => {
    const blocks = await sql<{ baseFeeSum: number | null; date: Date }[]>`
      SELECT date_trunc('hour', mined_at) AS date, SUM(base_fee_sum) AS base_fee_sum
      FROM base_fees_per_block
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

const notifyNewBaseFee = async (block: BlockLondon): Promise<void> => {
  const { feesBurnedAll: totalFeesBurned } = await getTotalFeesBurned();

  await sql.notify(
    "base-fee-updates",
    JSON.stringify({
      type: "base-fee-update",
      number: block.number,
      baseFeePerGas: hexToNumber(block.baseFeePerGas),
      fees: calcBlockBaseFeeSum(block),
      totalFeesBurned: totalFeesBurned,
    }),
  );
};

export const calcBlockFeeBreakdown = (
  block: BlockLondon,
  txrs: readonly TxRWeb3London[],
): FeeBreakdown => {
  const { contractCreationTxrs, ethTransferTxrs, contractUseTxrs } =
    Transactions.segmentTxrs(txrs);

  const ethTransferFees = pipe(
    ethTransferTxrs,
    A.map((txr) => calcTxrBaseFee(block, txr)),
    sum,
  );

  const contractCreationFees = pipe(
    contractCreationTxrs,
    A.map((txr) => calcTxrBaseFee(block, txr)),
    sum,
  );

  const feePerContract = calcBaseFeePerContract(block, contractUseTxrs);

  return {
    transfers: ethTransferFees,
    contract_use_fees: feePerContract,
    contract_creation_fees: contractCreationFees,
  };
};

export const calcBlockTips = (
  block: BlockLondon,
  txrs: readonly TxRWeb3London[],
): number => {
  return pipe(
    txrs,
    ROA.map(
      (txr) =>
        txr.gasUsed * hexToNumber(txr.effectiveGasPrice) -
        txr.gasUsed * hexToNumber(block.baseFeePerGas),
    ),
    sum,
  );
};

const blockAnalysisQueue = new PQueue({ concurrency: 8 });

const notifyNewBlock = async (block: BlockLondon): Promise<void> => {
  await sql.notify(
    "new-block",
    JSON.stringify({
      number: block.number,
    }),
  );
};

const calcBaseFeesForBlockNumber = (
  blockNumber: number,
  notify: boolean,
): T.Task<void> => {
  const calcBaseFeesTransaction = Sentry.startTransaction({
    op: "calc-base-fees",
    name: "calculate block base fees",
  });

  return pipe(
    () => {
      Log.debug(`> analyzing block ${blockNumber}`);
      return eth.getBlock(blockNumber);
    },
    T.chain((block) =>
      sequenceT(T.ApplyPar)(T.of(block), () =>
        Transactions.getTxrsWithRetry(block),
      ),
    ),
    T.chain(([block, txrs]) => {
      const feeBreakdown = calcBlockFeeBreakdown(block, txrs);
      const tips = calcBlockTips(block, txrs);
      const baseFeeSum = Number(block.baseFeePerGas) * block.gasUsed;

      Log.debug(
        `  fees burned for block ${blockNumber} - ${weiToEth(baseFeeSum)} ETH`,
      );

      if (process.env.SHOW_PROGRESS !== undefined) {
        DisplayProgress.onBlockAnalyzed();
      }

      return pipe(
        T.sequenceArray([
          notify ? () => notifyNewBaseFee(block) : T.of(undefined),
          notify ? () => notifyNewBlock(block) : T.of(undefined),
          () => storeBaseFeesForBlock(block, feeBreakdown, baseFeeSum, tips),
        ]),
        T.map(() => {
          calcBaseFeesTransaction.finish();
        }),
      );
    }),
  );
};

export const reanalyzeAllBlocks = async () => {
  Log.info("reanalyzing all blocks");
  await eth.webSocketOpen;

  const latestBlock = await eth.getBlock("latest");
  Log.debug(`latest block is ${latestBlock.number}`);

  const blocksToAnalyze = Blocks.getBlockRange(
    Blocks.londonHardForkBlockNumber,
    latestBlock.number,
  );

  if (process.env.SHOW_PROGRESS !== undefined) {
    DisplayProgress.start(blocksToAnalyze.length);
  }

  Log.debug(`${blocksToAnalyze.length} blocks to analyze`);

  await blockAnalysisQueue.addAll(
    blocksToAnalyze.map((blockNumber) =>
      calcBaseFeesForBlockNumber(blockNumber, false),
    ),
  );
};

export const watchAndCalcBaseFees = async () => {
  Log.info("watching and analyzing new blocks");

  await eth.webSocketOpen;

  eth.subscribeNewHeads((head) =>
    calcBaseFeesForBlockNumber(head.number, true)(),
  );

  Log.info("checking for missing blocks");
  const latestBlock = await eth.getBlock("latest");
  const knownBlockNumbers = await sql<{ number: number }[]>`
    SELECT number FROM base_fees_per_block
  `.then((rows) => new Set(rows.map((row) => row.number)));
  const wantedBlockRange = Blocks.getBlockRange(
    Blocks.londonHardForkBlockNumber,
    latestBlock.number,
  );

  const missingBlocks = wantedBlockRange.filter(
    (wantedBlockNumber) => !knownBlockNumbers.has(wantedBlockNumber),
  );

  if (missingBlocks.length !== 0) {
    Log.info(`${missingBlocks.length} missing blocks, fetching`);

    if (process.env.SHOW_PROGRESS !== undefined) {
      DisplayProgress.start(missingBlocks.length);
    }

    await blockAnalysisQueue.addAll(
      missingBlocks.map((blockNumber) =>
        calcBaseFeesForBlockNumber(blockNumber, false),
      ),
    );
    Log.info("done analysing missing blocks");
  } else {
    Log.info("no missing blocks");
  }
};

export type BurnRates = {
  burnRate1h: number;
  burnRate24h: number;
  burnRate7d: number;
  burnRate30d: number;
  burnRateAll: number;
};

export const getBurnRates = async () => {
  const burnRate1h = () =>
    sql<{ burnPerMinute: number }[]>`
      SELECT SUM(base_fee_sum) / (1 * 60) AS burn_per_minute FROM base_fees_per_block
      WHERE mined_at >= now() - interval '1 hours'
  `.then((rows) => rows[0]?.burnPerMinute ?? 0);

  const burnRate24h = () =>
    sql<{ burnPerMinute: number }[]>`
      SELECT SUM(base_fee_sum) / (24 * 60) AS burn_per_minute FROM base_fees_per_block
      WHERE mined_at >= now() - interval '24 hours'
  `.then((rows) => rows[0]?.burnPerMinute ?? 0);

  const burnRate7d = () =>
    sql<{ burnPerMinute: number }[]>`
      SELECT SUM(base_fee_sum) / (7 * 24 * 60) AS burn_per_minute FROM base_fees_per_block
      WHERE mined_at >= now() - interval '7 days'
  `.then((rows) => rows[0]?.burnPerMinute ?? 0);

  const burnRate30d = () =>
    sql<{ burnPerMinute: number }[]>`
      SELECT SUM(base_fee_sum) / (30 * 24 * 60) AS burn_per_minute FROM base_fees_per_block
      WHERE mined_at >= now() - interval '30 days'
  `.then((rows) => rows[0]?.burnPerMinute ?? 0);

  const burnRateAll = () =>
    sql<{ burnPerMinute: number }[]>`
      SELECT
        SUM(base_fee_sum) / (
          EXTRACT(
            epoch FROM (
              now() - '2021-08-05 12:33:42+00'
            )
          )
        ) AS burn_per_minute
      FROM base_fees_per_block
  `.then((rows) => rows[0]?.burnPerMinute ?? 0);

  return sequenceS(T.ApplyPar)({
    burnRate1h,
    burnRate24h,
    burnRate7d,
    burnRate30d,
    burnRateAll,
  })();
};
