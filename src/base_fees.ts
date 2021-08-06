import { sql } from "./db.js";
import type { TxRWeb3London } from "./transactions";
import A from "fp-ts/lib/Array.js";
import NEA from "fp-ts/lib/NonEmptyArray.js";
import R from "fp-ts/lib/Record.js";
import { flow, pipe } from "fp-ts/lib/function.js";
import * as Log from "./log.js";
import { hexToNumber, sum } from "./numbers.js";
import { getUnixTime, startOfDay } from "date-fns";
import { BlockLondon } from "./web3.js";
import neatCsv from "neat-csv";
import fs from "fs/promises";
import * as Transactions from "./transactions.js";
import * as eth from "./web3.js";
import Config from "./config.js";
import { delay } from "./delay.js";
import * as DisplayProgress from "./display_progress.js";
import PQueue from "p-queue";
import * as ROA from "fp-ts/lib/ReadonlyArray.js";
import * as Blocks from "./blocks.js";
import Sentry from "@sentry/node";

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
    SELECT max(number) AS number FROM base_fees_per_block
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
  tips: number,
): Promise<void> =>
  sql`
  INSERT INTO base_fees_per_block
    (hash, number, base_fees, mined_at, tips)
  VALUES
    (
      ${block.hash},
      ${block.number},
      ${sql.json(baseFees)},
      to_timestamp(${getBlockTimestamp(block)}),
      ${tips}
    )
  `.then(() => undefined);

// const toBaseFeeInsert = ({
//   block,
//   baseFees,
// }: {
//   block: BlockLondon;
//   baseFees: BlockBaseFees;
// }) => ({
//   hash: block.hash,
//   number: block.number,
//   base_fees: sql.json(baseFees),
//   mined_at: new Date(getBlockTimestamp(block) * 1000),
// });

// const storeBaseFeesForBlocks = async (
//   analyzedBlocks: { block: BlockLondon; baseFees: BlockBaseFees }[],
// ): Promise<void> => {
//   await sql`
//     INSERT INTO base_fees_per_block
//     ${sql(
//       analyzedBlocks.map(toBaseFeeInsert),
//       "hash",
//       "number",
//       "base_fees",
//       "mined_at",
//     )}
//   `;
// };

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

export type Timeframe = "24h" | "7d" | "30d" | "all";

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

const calcBlockBaseFeeSum = (baseFees: FeeBreakdown): number =>
  baseFees.transfers +
  baseFees.contract_creation_fees +
  sum(Object.values(baseFees.contract_use_fees) as number[]);

export const getTotalFeesBurned = async (): Promise<number> => {
  const baseFeesPerBlock = await sql<{ baseFees: FeeBreakdown }[]>`
      SELECT base_fees
      FROM base_fees_per_block
  `.then((rows) => {
    if (rows.length === 0) {
      Log.warn("tried to get top fee burners before any blocks were analyzed");
    }

    return rows.map((row) => row.baseFees);
  });

  return pipe(baseFeesPerBlock, A.map(calcBlockBaseFeeSum), sum);
};

export type FeesBurnedPerDay = Record<string, number>;

export const getFeesBurnedPerDay = async (): Promise<FeesBurnedPerDay> => {
  const blocks = await sql<{ baseFees: FeeBreakdown; minedAt: Date }[]>`
      SELECT base_fees, mined_at
      FROM base_fees_per_block
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
    NEA.groupBy((block) =>
      pipe(block.minedAt, startOfDay, getUnixTime, String),
    ),
    R.map(
      flow(
        NEA.map((block) => block.baseFees),
        NEA.map(calcBlockBaseFeeSum),
        sum,
      ),
    ),
  );
};

// Ideally callers get a quick answer, for this we need to keep a running total and update block by block. For now we do this in memory with a promise that is initially calculated from an expensive DB query and then updated block by block.
let totalFeesBurned: Promise<number> | undefined = undefined;
export const getRealtimeTotalFeesBurned = async (
  latestBlockBaseFees: FeeBreakdown,
) => {
  if (totalFeesBurned === undefined) {
    totalFeesBurned = new Promise((resolve) => {
      resolve(getTotalFeesBurned());
    });
    return totalFeesBurned;
  }

  totalFeesBurned = Promise.resolve(
    (await totalFeesBurned) + calcBlockBaseFeeSum(latestBlockBaseFees),
  );
  return totalFeesBurned;
};

export const notifyNewBaseFee = async (
  block: BlockLondon,
  latestBlockBaseFees: FeeBreakdown,
): Promise<void> => {
  // TODO: when running against mainnet pre-london we need to skip some blocks.
  if (block.baseFeePerGas === undefined) {
    return;
  }

  const totalFeesBurned = await getRealtimeTotalFeesBurned(latestBlockBaseFees);

  await sql.notify(
    "base-fee-updates",
    JSON.stringify({
      type: "base-fee-update",
      number: block.number,
      baseFeePerGas: hexToNumber(block.baseFeePerGas),
      fees: calcBlockBaseFeeSum(latestBlockBaseFees),
      totalFeesBurned,
    }),
  );

  return;
};

export const calcBlockBaseFees = (
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

const calcBaseFeesForBlockNumber = async (
  blockNumber: number,
): Promise<void> => {
  const calcBaseFeesTransaction = Sentry.startTransaction({
    op: "calc-base-fees",
    name: "calculate block base fees",
  });
  Log.debug(`analyzing block ${blockNumber}`);
  const block = await eth.getBlock(blockNumber);
  Log.debug(`  fetching ${block.transactions.length} transaction receipts`);
  const txrs = await Transactions.getTxrs1559(block.transactions);
  const feeBreakdown = calcBlockBaseFees(block, txrs);
  const tips = calcBlockTips(block, txrs);
  const baseFeesSum = calcBlockBaseFeeSum(feeBreakdown);

  Log.debug(`  fees burned for block ${blockNumber} - ${baseFeesSum} wei`);

  if (process.env.ENV === "dev" && process.env.SHOW_PROGRESS !== undefined) {
    DisplayProgress.onBlockAnalyzed();
  }

  await storeBaseFeesForBlock(block, feeBreakdown, tips);
  await notifyNewBaseFee(block, feeBreakdown);
  calcBaseFeesTransaction.finish();
};

export const watchAndCalcBaseFees = async () => {
  Log.info("starting gas analysis");
  await eth.webSocketOpen;

  // eslint-disable-next-line no-constant-condition
  while (true) {
    const latestAnalyzedBlockNumber = await getLatestAnalyzedBlockNumber();
    const latestBlock = await eth.getBlock("latest");
    Log.debug(`latest block is ${latestBlock.number}`);

    // Figure out which blocks we'd like to analyze.
    const nextToAnalyze =
      latestAnalyzedBlockNumber !== undefined
        ? latestAnalyzedBlockNumber + 1
        : Blocks.londonHardForkBlockNumber;

    const blocksToAnalyze = Blocks.getBlockRange(
      nextToAnalyze,
      latestBlock.number,
    );

    if (Config.env === "dev" && process.env.SHOW_PROGRESS !== undefined) {
      DisplayProgress.start(blocksToAnalyze.length);
    }

    if (blocksToAnalyze.length === 0) {
      Log.debug("no new blocks to analyze");
    } else {
      Log.info(`${blocksToAnalyze.length} blocks to analyze`);
    }

    await blockAnalysisQueue.addAll(
      blocksToAnalyze.map(
        (blockNumber) => () => calcBaseFeesForBlockNumber(blockNumber),
      ),
    );

    // Wait 1s before checking for new blocks to analyze
    await delay(2000);
  }
};
