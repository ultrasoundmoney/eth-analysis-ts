import * as Leaderboards from "./leaderboards.js";
import * as Log from "./log.js";
import {
  LeaderboardEntry,
  LeaderboardRow,
  LimitedTimeframe,
} from "./leaderboards.js";
import { A, Ord, pipe, T } from "./fp.js";
import { fromUnixTime, isAfter, subHours } from "date-fns";
import { seqSPar, seqTPar } from "./sequence.js";
import { sql } from "./db.js";
import { FeeBreakdown } from "./base_fees.js";
import { BlockLondon } from "./eth_node.js";
import { performance } from "perf_hooks";

type ContractAddress = string;

type BlockForTotal = { number: number; minedAt: Date };

type SumForTotal = { contractAddress: string; baseFeeSum: number };

type BlocksPerTimeframe = Record<LimitedTimeframe, BlockForTotal[]>;

type ContractSumsPerTimeframe = Record<
  LimitedTimeframe,
  Record<ContractAddress, number>
>;

const blocksPerTimeframe: BlocksPerTimeframe = {
  "1h": [],
  "24h": [],
  "7d": [],
  "30d": [],
};
const contractSumsPerTimeframe: ContractSumsPerTimeframe = {
  "1h": {},
  "24h": {},
  "7d": {},
  "30d": {},
};

type SyncStatus = "unknown" | "in-sync" | "out-of-sync";
let syncStatus: SyncStatus = "unknown";

export const getSyncStatus = (): SyncStatus => syncStatus;
export const setSyncStatus = (newSyncStatus: SyncStatus): void => {
  syncStatus = newSyncStatus;
};

const getBlocksForTimeframe = (
  timeframe: LimitedTimeframe,
  upToIncluding: number,
): T.Task<BlockForTotal[]> => {
  const hours = Leaderboards.timeframeHoursMap[timeframe];

  return () =>
    sql<BlockForTotal[]>`
      SELECT number, mined_at FROM blocks
      WHERE mined_at >= NOW() - interval '${sql(String(hours))} hours'
      AND number <= ${upToIncluding}
      ORDER BY (number) ASC
    `;
};

const getTotalsForBlocks = (
  from: number,
  upToIncluding: number,
): T.Task<Record<ContractAddress, number>> => {
  return pipe(
    () =>
      sql<SumForTotal[]>`
        SELECT contract_address, SUM(base_fees) AS base_fee_sum
        FROM contract_base_fees
        WHERE block_number >= ${from}
        AND block_number <= ${upToIncluding}
        GROUP BY (contract_address)
      `,
    T.map(A.map((row) => [row.contractAddress, row.baseFeeSum])),
    T.map(Object.fromEntries),
  );
};
type BaseFeesToAdd = Record<ContractAddress, number>;

const addToRunningSums = (
  timeframe: LimitedTimeframe,
  baseFeesToAdd: BaseFeesToAdd,
): ContractSumsPerTimeframe => {
  const contractSums = contractSumsPerTimeframe[timeframe];

  Object.entries(baseFeesToAdd).forEach(([contractAddress, baseFees]) => {
    const currentBaseFeeSum = contractSums[contractAddress] || 0;
    contractSums[contractAddress] = currentBaseFeeSum + baseFees;
  });

  return contractSumsPerTimeframe;
};

const removeFromRunningSums = (
  timeframe: LimitedTimeframe,
  baseFeesToRemove: BaseFeesToAdd,
): ContractSumsPerTimeframe => {
  const contractSums = contractSumsPerTimeframe[timeframe];

  Object.entries(baseFeesToRemove).forEach(([contractAddress, baseFees]) => {
    const currentBaseFeeSum = contractSums[contractAddress];
    if (currentBaseFeeSum === undefined) {
      throw new Error("tried to remove base fees from a non-existing sum");
    }
    contractSums[contractAddress] = currentBaseFeeSum - baseFees;
  });

  return contractSumsPerTimeframe;
};

const blockForTotalOrd: Ord<BlockForTotal> = {
  equals: (x, y) => x.number === y.number,
  compare: (x, y) => (x.number < y.number ? -1 : 1),
};

const addAllBlocks = (
  timeframe: LimitedTimeframe,
  upToIncluding: number,
): T.Task<void> => {
  const t0 = performance.now();
  Log.debug(`loading sums for ${timeframe}`);
  const includedBlocks = blocksPerTimeframe[timeframe];
  return pipe(
    getBlocksForTimeframe(timeframe, upToIncluding),
    T.chain((blocks) =>
      seqTPar(
        T.of(blocks),
        getTotalsForBlocks(blocks[0].number, blocks[blocks.length - 1].number),
      ),
    ),
    T.map(([blocks, sums]) => {
      blocksPerTimeframe[timeframe] = pipe(
        includedBlocks,
        A.concat(blocks),
        A.sort(blockForTotalOrd),
      );
      addToRunningSums(timeframe, sums);
      const t1 = performance.now();
      const took = Number((t1 - t0) / 1000).toFixed(2);
      Log.debug(`loading leaderboard ${timeframe} took ${took}s`);
    }),
    T.map(() => undefined),
  );
};

export const addAllBlocksForAllTimeframes = (
  upToIncluding: number,
): T.Task<void> =>
  pipe(
    seqTPar(
      addAllBlocks("1h", upToIncluding),
      addAllBlocks("24h", upToIncluding),
      addAllBlocks("7d", upToIncluding),
      addAllBlocks("30d", upToIncluding),
    ),
    T.map(() => undefined),
  );

const addBlockForTimeframe = (
  timeframe: LimitedTimeframe,
  block: BlockLondon,
  feeBreakdown: FeeBreakdown,
): void => {
  const includedBlocks = blocksPerTimeframe[timeframe];
  includedBlocks.push({
    number: block.number,
    minedAt: fromUnixTime(block.timestamp),
  });
  includedBlocks.sort(blockForTotalOrd.compare);
  addToRunningSums(timeframe, feeBreakdown.contract_use_fees);
};

export const addBlockForAllTimeframes = (
  block: BlockLondon,
  feeBreakdown: FeeBreakdown,
): void => {
  addBlockForTimeframe("1h", block, feeBreakdown);
  addBlockForTimeframe("24h", block, feeBreakdown);
  addBlockForTimeframe("7d", block, feeBreakdown);
  addBlockForTimeframe("30d", block, feeBreakdown);
};

export const removeExpiredBlocksFromSums = (
  timeframe: LimitedTimeframe,
): T.Task<[BlocksPerTimeframe, ContractSumsPerTimeframe]> => {
  const includedBlocks = blocksPerTimeframe[timeframe];
  const ageLimit = subHours(
    new Date(),
    Leaderboards.timeframeHoursMap[timeframe],
  );
  const youngEnoughIndex = includedBlocks.findIndex((block) =>
    isAfter(ageLimit, block.minedAt),
  );

  if (youngEnoughIndex === -1) {
    return T.of([blocksPerTimeframe, contractSumsPerTimeframe]);
  }

  const blocksToRemove = includedBlocks.slice(0, youngEnoughIndex + 1);

  const blocksToRemoveStr = blocksToRemove
    .map((block) => block.number)
    .join(",");
  Log.debug(
    `some blocks are too old in ${timeframe} timeframe, removing ${blocksToRemoveStr}`,
  );
  blocksPerTimeframe[timeframe] = includedBlocks.slice(youngEnoughIndex + 1);
  return pipe(
    getTotalsForBlocks(
      blocksToRemove[0].number,
      blocksToRemove[blocksToRemove.length - 1].number,
    ),
    T.map((baseFeesToRemove) =>
      removeFromRunningSums(timeframe, baseFeesToRemove),
    ),
    T.map((newContractSumsPerTimeframe) => [
      blocksPerTimeframe,
      newContractSumsPerTimeframe,
    ]),
  );
};

export const removeExpiredBlocksFromSumsForAllTimeframes = (): T.Task<void> => {
  return pipe(
    seqTPar(
      removeExpiredBlocksFromSums("1h"),
      removeExpiredBlocksFromSums("24h"),
      removeExpiredBlocksFromSums("7d"),
      removeExpiredBlocksFromSums("30d"),
    ),
    T.map(() => undefined),
  );
};

const getTopBaseFeeContracts = (
  timeframe: LimitedTimeframe,
): T.Task<LeaderboardRow[]> => {
  const contractSums = contractSumsPerTimeframe[timeframe];
  const topAddresses = pipe(
    contractSums,
    Object.entries,
    A.sort<[string, number]>({
      equals: ([, baseFeeA], [, baseFeeB]) => baseFeeA === baseFeeB,
      compare: ([, baseFeeA], [, baseFeeB]) => (baseFeeA < baseFeeB ? 1 : -1),
    }),
    A.takeLeft(24),
    A.map(([address]) => address),
  );
  return pipe(
    () =>
      sql<{ address: string; name: string; isBot: boolean }[]>`
      SELECT address, name, is_bot
      FROM contracts
      WHERE address IN (${topAddresses})
    `,
    T.map(
      A.map((row) => ({
        contractAddress: row.address,
        name: row.name,
        isBot: row.isBot,
        baseFees: contractSums[row.address],
      })),
    ),
  );
};

const calcLeaderboardForLimitedTimeframe = (
  timeframe: LimitedTimeframe,
): T.Task<LeaderboardEntry[]> => {
  return pipe(
    seqTPar(
      getTopBaseFeeContracts(timeframe),
      () => Leaderboards.getEthTransferFeesForTimeframe(timeframe),
      () => Leaderboards.getContractCreationBaseFeesForTimeframe(timeframe),
    ),
    T.map(([contractUse, ethTransfer, contractCreation]) =>
      Leaderboards.buildLeaderboard(contractUse, ethTransfer, contractCreation),
    ),
  );
};

export const calcLeaderboardForLimitedTimeframes = (): T.Task<
  Record<LimitedTimeframe, LeaderboardEntry[]>
> =>
  seqSPar({
    "1h": calcLeaderboardForLimitedTimeframe("1h"),
    "24h": calcLeaderboardForLimitedTimeframe("24h"),
    "7d": calcLeaderboardForLimitedTimeframe("7d"),
    "30d": calcLeaderboardForLimitedTimeframe("30d"),
  });
