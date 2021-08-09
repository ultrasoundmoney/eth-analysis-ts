import { sql } from "./db.js";
import type { TxRWeb3London } from "./transactions";
import A from "fp-ts/lib/Array.js";
import NEA from "fp-ts/lib/NonEmptyArray.js";
import R from "fp-ts/lib/Record.js";
import { flow, pipe } from "fp-ts/lib/function.js";
import * as Log from "./log.js";
import { hexToNumber, sum, weiToEth } from "./numbers.js";
import { getUnixTime, startOfDay } from "date-fns";
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
import { sequenceT } from "fp-ts/lib/Apply.js";

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

const calcBlockBaseFeeSum = (block: BlockLondon): number =>
  block.gasUsed * hexToNumber(block.baseFeePerGas);

export const getTotalFeesBurned = async (): Promise<number> => {
  const baseFeeSum = await sql<{ baseFeeSum: number }[]>`
      SELECT SUM(base_fee_sum) as base_fee_sum FROM base_fees_per_block
  `.then((rows) => {
    if (rows.length === 0) {
      Log.warn("tried to get top fee burners before any blocks were analyzed");
    }

    return rows[0]?.baseFeeSum ?? 0;
  });

  return baseFeeSum;
};

export type FeesBurnedPerDay = Record<string, number>;

export const getFeesBurnedPerDay = async (): Promise<FeesBurnedPerDay> => {
  const blocks = await sql<{ baseFeeSum: number; minedAt: Date }[]>`
      SELECT base_fee_sum, mined_at
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
        NEA.map((block) => block.baseFeeSum),
        sum,
      ),
    ),
  );
};

export const notifyNewBaseFee = async (block: BlockLondon): Promise<void> => {
  await sql.notify(
    "base-fee-updates",
    JSON.stringify({
      type: "base-fee-update",
      number: block.number,
      baseFeePerGas: hexToNumber(block.baseFeePerGas),
      fees: calcBlockBaseFeeSum(block),
      totalFeesBurned: await getTotalFeesBurned(),
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

const calcBaseFeesForBlockNumber = (blockNumber: number): T.Task<void> => {
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
          () => notifyNewBaseFee(block),
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
    blocksToAnalyze.map(calcBaseFeesForBlockNumber),
  );
};

export const watchAndCalcBaseFees = async () => {
  Log.info("watching and analyzing new blocks");
  eth.subscribeNewHeads((head) => calcBaseFeesForBlockNumber(head.number)());
};
