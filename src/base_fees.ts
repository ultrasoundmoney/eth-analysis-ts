import { sql } from "./db.js";
import type { TxRWeb3London } from "./transactions";
import A from "fp-ts/lib/Array.js";
import NEA from "fp-ts/lib/NonEmptyArray.js";
import R from "fp-ts/lib/Record.js";
import { pipe } from "fp-ts/lib/function.js";
import * as Log from "./log.js";
import { sum } from "./numbers.js";
import { BlockLondon } from "./web3.js";
import neatCsv from "neat-csv";
import fs from "fs/promises";
import * as Transactions from "./transactions.js";
import * as eth from "./web3.js";
import * as DisplayProgress from "./display_progress.js";
import PQueue from "p-queue";
import * as ROA from "fp-ts/lib/ReadonlyArray.js";
import * as Blocks from "./blocks.js";
import * as T from "fp-ts/lib/Task.js";
import { sequenceT, sequenceS } from "fp-ts/lib/Apply.js";
import { hexToNumber } from "./hexadecimal.js";
import { weiToEth } from "./convert_unit.js";
import { fromUnixTime, isAfter, subDays, subHours } from "date-fns";
import * as Contracts from "./contracts.js";

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
  base_fee_sum: calcBlockBaseFeeSum(block),
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

const updateBlockBaseFees = async (
  block: BlockLondon,
  txrs: TxRWeb3London[],
  tips: number,
): Promise<void> => {
  const feeBreakdown = calcBlockFeeBreakdown(block, txrs);
  const blockRow = getBlockRow(block, feeBreakdown, tips);
  const contractBaseFeesRows = getContractRows(block, feeBreakdown);

  const updateBlockTask = () =>
    sql`
      UPDATE base_fees_per_block
      SET
        ${sql(blockRow)}
      WHERE
        number = ${block.number}
    `.then(() => undefined);

  const updateContractBaseFeesTask = () =>
    sql.begin(async (sql) => {
      await sql`DELETE FROM contract_base_fees WHERE block_number = ${block.number}`;
      if (txrs.length !== 0) {
        await sql`INSERT INTO contract_base_fees ${sql(contractBaseFeesRows)}`;
      }
    });

  return pipe(
    T.sequenceArray([updateBlockTask, updateContractBaseFeesTask]),
    T.map(() => undefined),
  )();
};

const insertBlockBaseFees = async (
  block: BlockLondon,
  txrs: TxRWeb3London[],
  tips: number,
): Promise<void> => {
  const feeBreakdown = calcBlockFeeBreakdown(block, txrs);
  const blockRow = getBlockRow(block, feeBreakdown, tips);
  const contractBaseFeesRows = getContractRows(block, feeBreakdown);

  const insertTask = () =>
    sql`
    INSERT INTO base_fees_per_block
      ${sql(blockRow)}
  `.then(() => undefined);

  if (contractBaseFeesRows.length === 0) {
    await insertTask();
    return;
  }

  const addresses = contractBaseFeesRows.map(
    (contractBurnRow) => contractBurnRow.contract_address,
  );
  const insertContractsTask = () => Contracts.insertContracts(addresses);

  const insertContractBaseFeesTask = () =>
    sql`
      INSERT INTO contract_base_fees ${sql(contractBaseFeesRows)}
    `.then(() => undefined);

  await T.sequenceSeqArray([
    T.sequenceArray([insertTask, insertContractsTask]),
    insertContractBaseFeesTask,
  ])();
};

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
export type LeaderboardEntry = {
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

export type Timeframe = LimitedTimeframe | "all";
export type LimitedTimeframe = "1h" | "24h" | "7d" | "30d";

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

type FeeBurnBlock = {
  baseFeeSum: number;
  minedAt: Date;
};

type TotalFeeBurnCache = {
  feesBurned1h: FeeBurnBlock[];
  feesBurned24h: FeeBurnBlock[];
  feesBurned7d: FeeBurnBlock[];
  feesBurned30d: FeeBurnBlock[];
  feesBurnedAll: number;
};

let totalFeeBurnCache: undefined | TotalFeeBurnCache = undefined;

const getBlocksYoungerThan = (
  date: Date,
  blocks: FeeBurnBlock[],
): FeeBurnBlock[] => {
  let youngerIndex = 0;
  for (const block of blocks) {
    if (isAfter(block.minedAt, date)) {
      break;
    }
    youngerIndex = youngerIndex + 1;
  }

  return blocks.slice(youngerIndex);
};

const getFeeBurnFromDb = async (): Promise<TotalFeeBurnCache> => {
  const feesBurned1h = () =>
    sql<{ baseFeeSum: number; minedAt: Date }[]>`
      SELECT base_fee_sum, mined_at FROM base_fees_per_block
      WHERE mined_at >= now() - interval '1 hours'
  `;

  const feesBurned24h = () =>
    sql<{ baseFeeSum: number; minedAt: Date }[]>`
      SELECT base_fee_sum, mined_at FROM base_fees_per_block
      WHERE mined_at >= now() - interval '24 hours'
  `;

  const feesBurned7d = () =>
    sql<{ baseFeeSum: number; minedAt: Date }[]>`
      SELECT base_fee_sum, mined_at FROM base_fees_per_block
      WHERE mined_at >= now() - interval '7 days'
  `;

  const feesBurned30d = () =>
    sql<{ baseFeeSum: number; minedAt: Date }[]>`
      SELECT base_fee_sum, mined_at FROM base_fees_per_block
      WHERE mined_at >= now() - interval '30 days'
  `;

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

export const updateTotalFeeBurnCache = async (
  block: BlockLondon,
): Promise<void> => {
  if (totalFeeBurnCache === undefined) {
    const feeBurn = await getFeeBurnFromDb();
    totalFeeBurnCache = feeBurn;
  }

  const now = new Date();
  const nowMin1h = subHours(now, 1);
  const nowMin24h = subHours(now, 24);
  const nowMin7d = subDays(now, 7);
  const nowMin30d = subDays(now, 30);

  const baseFeeSum = calcBlockBaseFeeSum(block);
  const newFeeBurnBlock = {
    baseFeeSum,
    minedAt: fromUnixTime(block.timestamp),
  };

  const feesBurned1h = getBlocksYoungerThan(
    nowMin1h,
    totalFeeBurnCache.feesBurned1h,
  );
  feesBurned1h.push(newFeeBurnBlock);

  const feesBurned24h = getBlocksYoungerThan(
    nowMin24h,
    totalFeeBurnCache.feesBurned24h,
  );
  feesBurned24h.push(newFeeBurnBlock);

  const feesBurned7d = getBlocksYoungerThan(
    nowMin7d,
    totalFeeBurnCache.feesBurned7d,
  );
  feesBurned7d.push(newFeeBurnBlock);

  const feesBurned30d = getBlocksYoungerThan(
    nowMin30d,
    totalFeeBurnCache.feesBurned30d,
  );
  feesBurned30d.push(newFeeBurnBlock);

  const feesBurnedAll = totalFeeBurnCache.feesBurnedAll + baseFeeSum;

  totalFeeBurnCache = {
    // We can assume totalFeeBurnCache is defined because we called getTotalFeesBurned above.
    feesBurned1h,
    feesBurned24h,
    feesBurned7d,
    feesBurned30d,
    feesBurnedAll,
  };
};

const sumFeeBurnBlocks = (feeBurnBlocks: FeeBurnBlock[]): number =>
  feeBurnBlocks.reduce((sum, block) => {
    return sum + block.baseFeeSum;
  }, 0);

export const getTotalFeesBurned = (): FeesBurned => {
  if (totalFeeBurnCache === undefined) {
    throw new Error(
      "only call get total fees burned after at least one block update",
    );
  }

  return {
    feesBurned1h: sumFeeBurnBlocks(totalFeeBurnCache.feesBurned1h),
    feesBurned24h: sumFeeBurnBlocks(totalFeeBurnCache.feesBurned24h),
    feesBurned7d: sumFeeBurnBlocks(totalFeeBurnCache.feesBurned7d),
    feesBurned30d: sumFeeBurnBlocks(totalFeeBurnCache.feesBurned30d),
    feesBurnedAll: totalFeeBurnCache.feesBurnedAll,
  };
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

const parBlockAnalysisQueue = new PQueue({ concurrency: 8 });
const seqBlockAnalysisQueue = new PQueue({ concurrency: 1 });

export type NewBlockPayload = {
  number: number;
};
const notifyNewBlock = async (block: BlockLondon): Promise<void> => {
  const payload: NewBlockPayload = {
    number: block.number,
  };

  await sql.notify("new-block", JSON.stringify(payload));
};

// We try to get away with tracking what blocks we've seen in a simple set. If this results in errors start checking against the DB.
const knownBlocks = new Set<number>();

const calcBaseFeesForBlockNumber = (
  blockNumber: number,
  notify: boolean,
): T.Task<void> =>
  pipe(
    () => {
      Log.info(`> analyzing block ${blockNumber}`);
      return eth.getBlock(blockNumber);
    },
    T.chain((block) =>
      sequenceT(T.ApplyPar)(T.of(block), () =>
        Transactions.getTxrsWithRetry(block),
      ),
    ),
    T.chain(([block, txrs]) => {
      const tips = calcBlockTips(block, txrs);
      const baseFeeSum = Number(block.baseFeePerGas) * block.gasUsed;

      Log.debug(
        `  fees burned for block ${blockNumber} - ${weiToEth(baseFeeSum)} ETH`,
      );

      if (process.env.SHOW_PROGRESS !== undefined) {
        DisplayProgress.onBlockAnalyzed();
      }

      return pipe(
        () =>
          knownBlocks.has(blockNumber)
            ? updateBlockBaseFees(block, txrs, tips)
            : insertBlockBaseFees(block, txrs, tips),
        T.map(() => {
          knownBlocks.add(blockNumber);
        }),
        T.chain(() => (notify ? () => notifyNewBlock(block) : T.of(undefined))),
      );
    }),
  );

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

  await parBlockAnalysisQueue.addAll(
    blocksToAnalyze.map((blockNumber) =>
      calcBaseFeesForBlockNumber(blockNumber, false),
    ),
  );
};

export const watchAndCalcBaseFees = async () => {
  Log.info("watching and analyzing new blocks");

  await eth.webSocketOpen;

  eth.subscribeNewHeads((head) =>
    seqBlockAnalysisQueue.add(calcBaseFeesForBlockNumber(head.number, true)),
  );

  await analyzeMissingBlocks();
};

export const analyzeMissingBlocks = async () => {
  Log.info("checking for missing blocks");
  await eth.webSocketOpen;

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

    await parBlockAnalysisQueue.addAll(
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

  // The more complex queries account for the fact we don't have all blocks in the queried period yet and can't assume the amount of minutes is the length of the period in days times the number of minutes in a day. Once we do we can simplify to the above.
  const burnRate7d = () =>
    sql<{ burnPerMinute: number }[]>`
      SELECT
        SUM(base_fee_sum) / (
          EXTRACT(epoch FROM now() - min(mined_at)) / 60
        ) AS burn_per_minute
      FROM base_fees_per_block
      WHERE mined_at >= now() - interval '7 days'
  `.then((rows) => rows[0]?.burnPerMinute ?? 0);

  const burnRate30d = () =>
    sql<{ burnPerMinute: number }[]>`
      SELECT
        SUM(base_fee_sum) / (
          EXTRACT(epoch FROM now() - min(mined_at)) / 60
        ) AS burn_per_minute
      FROM base_fees_per_block
      WHERE mined_at >= now() - interval '30 days'
  `.then((rows) => rows[0]?.burnPerMinute ?? 0);

  const burnRateAll = () =>
    sql<{ burnPerMinute: number }[]>`
      SELECT
        SUM(base_fee_sum) / (
          EXTRACT(epoch FROM now() - '2021-08-05 12:33:42+00') / 60
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
