import * as Leaderboards from "./leaderboards.js";
import * as Log from "./log.js";
import { A, O, Ord, pipe, T } from "./fp.js";
import { fromUnixTime, isAfter, subHours } from "date-fns";
import { seqSPar, seqTPar } from "./sequence.js";
import { sql } from "./db.js";
import { BlockLondon } from "./eth_node.js";
import { performance } from "perf_hooks";
import {
  AddedBaseFeesLog,
  LeaderboardEntry,
  LeaderboardRow,
  LimitedTimeframe,
} from "./leaderboards.js";

type ContractAddress = string;

type BlockForTotal = { number: number; minedAt: Date };

type ContractBaseFeesRow = {
  contractAddress: ContractAddress;
  baseFees: number;
};

type BlocksPerTimeframe = Record<LimitedTimeframe, BlockForTotal[]>;

type ContractSumsPerTimeframe = Record<
  LimitedTimeframe,
  Map<ContractAddress, number>
>;

const blocksPerTimeframe: BlocksPerTimeframe = {
  "1h": [],
  "24h": [],
  "7d": [],
  "30d": [],
};
const contractSumsPerTimeframe: ContractSumsPerTimeframe = {
  "1h": new Map(),
  "24h": new Map(),
  "7d": new Map(),
  "30d": new Map(),
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

const getBaseFeesForRange = (
  from: number,
  upToIncluding: number,
): T.Task<ContractBaseFees> => {
  return pipe(
    () =>
      sql<ContractBaseFeesRow[]>`
        SELECT contract_address, SUM(base_fees) AS base_fees
        FROM contract_base_fees
        WHERE block_number >= ${from}
        AND block_number <= ${upToIncluding}
        GROUP BY (contract_address)
      `,
    T.map(collectInMap),
  );
};

type ContractBaseFees = Map<ContractAddress, number>;

const collectInMap = (rows: ContractBaseFeesRow[]): ContractBaseFees =>
  pipe(
    rows,

    A.map((row) => [row.contractAddress, row.baseFees] as [string, number]),
    (entries) => new Map(entries),
  );

const addToRunningSums = (
  timeframe: LimitedTimeframe,
  baseFeesToAdd: ContractBaseFees,
): void => {
  const contractSums = contractSumsPerTimeframe[timeframe];

  baseFeesToAdd.forEach((baseFees, contractAddress) => {
    const currentBaseFeeSum = contractSums.get(contractAddress) || 0;
    contractSums.set(contractAddress, currentBaseFeeSum + baseFees);
  });
};

const removeFromRunningSums = (
  timeframe: LimitedTimeframe,
  baseFeesToRemove: ContractBaseFees,
): void => {
  const contractSums = contractSumsPerTimeframe[timeframe];

  Object.entries(baseFeesToRemove).forEach(([contractAddress, baseFees]) => {
    const currentBaseFeeSum = contractSums.get(contractAddress);
    if (currentBaseFeeSum === undefined) {
      throw new Error("tried to remove base fees from a non-existing sum");
    }
    contractSums.set(contractAddress, currentBaseFeeSum - baseFees);
  });
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
  return pipe(
    getBlocksForTimeframe(timeframe, upToIncluding),
    T.chain((blocks) =>
      seqTPar(
        T.of(blocks),
        getBaseFeesForRange(blocks[0].number, blocks[blocks.length - 1].number),
      ),
    ),
    T.map(([blocks, sums]) => {
      blocksPerTimeframe[timeframe] = pipe(
        blocksPerTimeframe[timeframe],
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

let addedRowsLog: AddedBaseFeesLog[] = [];

const rollbackToBefore = (blockNumber: number): void => {
  const indexOfBlockToRollbackToBefore = blocksPerTimeframe["1h"].findIndex(
    (block) => block.number === blockNumber,
  );
  const blocksToRollback = blocksPerTimeframe["1h"].slice(
    indexOfBlockToRollbackToBefore,
  );
  if (blocksToRollback.length === 0) {
    Log.warn("tried to rollback empty timeframe");
  }

  const baseFeesToRollback = Leaderboards.getPreviouslyAddedSums(
    addedRowsLog,
    blocksToRollback[0].number,
    blocksToRollback[blocksToRollback.length - 1].number,
  );

  removeFromRunningSums("1h", baseFeesToRollback);
  removeFromRunningSums("24h", baseFeesToRollback);
  removeFromRunningSums("7d", baseFeesToRollback);
  removeFromRunningSums("30d", baseFeesToRollback);

  addedRowsLog = addedRowsLog.slice(0, -blocksToRollback.length);
};

const addBlockForTimeframe = (
  timeframe: LimitedTimeframe,
  block: BlockLondon,
  baseFeesToAdd: ContractBaseFees,
): void => {
  const includedBlocks = blocksPerTimeframe[timeframe];
  const shouldRollback = pipe(
    includedBlocks,
    A.last,
    O.match(
      () => {
        Log.warn(
          `limited timeframe ${timeframe} is empty, assuming no rollback`,
        );
        return false;
      },
      (lastStoredBlock) => block.number <= lastStoredBlock.number,
    ),
  );

  if (shouldRollback) {
    rollbackToBefore(block.number);
  }

  includedBlocks.push({
    number: block.number,
    minedAt: fromUnixTime(block.timestamp),
  });
  includedBlocks.sort(blockForTotalOrd.compare);
  addToRunningSums(timeframe, baseFeesToAdd);
};

const logAddedBaseFees = (
  blockNumber: number,
  baseFees: ContractBaseFees,
): void => {
  pipe(
    addedRowsLog,
    A.append({ blockNumber, baseFees }),
    A.takeRight(50),
    (rows) => {
      addedRowsLog = rows;
    },
  );
};

export const addBlockForAllTimeframes = (
  block: BlockLondon,
  baseFeesToAdd: ContractBaseFees,
): void => {
  logAddedBaseFees(block.number, baseFeesToAdd);
  addBlockForTimeframe("1h", block, baseFeesToAdd);
  addBlockForTimeframe("24h", block, baseFeesToAdd);
  addBlockForTimeframe("7d", block, baseFeesToAdd);
  addBlockForTimeframe("30d", block, baseFeesToAdd);
};

export const removeExpiredBlocksFromSums = (
  timeframe: LimitedTimeframe,
): T.Task<void> => {
  const includedBlocks = blocksPerTimeframe[timeframe];
  const ageLimit = subHours(
    new Date(),
    Leaderboards.timeframeHoursMap[timeframe],
  );
  const youngEnoughIndex = includedBlocks.findIndex((block) =>
    isAfter(ageLimit, block.minedAt),
  );

  if (youngEnoughIndex === -1) {
    // All blocks are still valid for the given timeframe.
    return T.of(undefined);
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
    getBaseFeesForRange(
      blocksToRemove[0].number,
      blocksToRemove[blocksToRemove.length - 1].number,
    ),
    T.map((baseFeesToRemove) =>
      removeFromRunningSums(timeframe, baseFeesToRemove),
    ),
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
    Array.from(contractSums.entries()),
    A.sort<[string, number]>({
      equals: ([, baseFeeA], [, baseFeeB]) => baseFeeA === baseFeeB,
      compare: ([, baseFeeA], [, baseFeeB]) => (baseFeeA < baseFeeB ? 1 : -1),
    }),
    A.takeLeft(24),
    A.map(([address]) => address),
  );

  type ContractRow = { address: string; name: string; isBot: boolean };

  return pipe(
    () =>
      sql<ContractRow[]>`
        SELECT address, name, is_bot
        FROM contracts
        WHERE address IN (${topAddresses})
      `,
    T.map(
      A.map((row) => ({
        contractAddress: row.address,
        name: row.name,
        isBot: row.isBot,
        baseFees: contractSums.get(row.address)!,
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
