import * as Leaderboards from "./leaderboards.js";
import * as Log from "./log.js";
import { A, O, Ord, pipe, seqSParT, seqTParT, T } from "./fp.js";
import { fromUnixTime, isAfter, subMinutes } from "date-fns";
import { sql } from "./db.js";
import { BlockLondon } from "./eth_node.js";
import { performance } from "perf_hooks";
import {
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
  "5m": [],
  "1h": [],
  "24h": [],
  "7d": [],
  "30d": [],
};
const contractSumsPerTimeframe: ContractSumsPerTimeframe = {
  "5m": new Map(),
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
  const minutes = Leaderboards.timeframeMinutesMap[timeframe];
  return () =>
    sql<BlockForTotal[]>`
      SELECT number, mined_at FROM blocks
      WHERE mined_at >= NOW() - interval '${sql(String(minutes))} minutes'
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
  for (const [contractAddress, baseFees] of baseFeesToRemove.entries()) {
    const currentBaseFeeSum = contractSums.get(contractAddress);
    if (currentBaseFeeSum === undefined) {
      throw new Error("tried to remove base fees from a non-existing sum");
    }
    contractSums.set(contractAddress, currentBaseFeeSum - baseFees);
  }
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
      pipe(
        O.sequenceArray([A.head(blocks), A.last(blocks)]),
        O.match(
          () => {
            Log.warn(
              `zero blocks in blocks table for interval now - ${timeframe}, skipping ${timeframe} fast leaderboard init`,
            );
            return T.of(undefined);
          },
          ([head, last]) =>
            pipe(
              getBaseFeesForRange(head.number, last.number),
              T.map((sums) => {
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
            ),
        ),
      ),
    ),
    T.map(() => undefined),
  );
};

export const addAllBlocksForAllTimeframes = (
  upToIncluding: number,
): T.Task<void> =>
  pipe(
    seqTParT(
      addAllBlocks("5m", upToIncluding),
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
  baseFeesToAdd: ContractBaseFees,
): void => {
  const includedBlocks = blocksPerTimeframe[timeframe];
  includedBlocks.push({
    number: block.number,
    minedAt: fromUnixTime(block.timestamp),
  });
  includedBlocks.sort(blockForTotalOrd.compare);
  addToRunningSums(timeframe, baseFeesToAdd);
};

export const addBlockForAllTimeframes = (
  block: BlockLondon,
  baseFeesToAdd: ContractBaseFees,
): void => {
  addBlockForTimeframe("5m", block, baseFeesToAdd);
  addBlockForTimeframe("1h", block, baseFeesToAdd);
  addBlockForTimeframe("24h", block, baseFeesToAdd);
  addBlockForTimeframe("7d", block, baseFeesToAdd);
  addBlockForTimeframe("30d", block, baseFeesToAdd);
};

export const removeExpiredBlocksFromSums = (
  timeframe: LimitedTimeframe,
): T.Task<void> => {
  const includedBlocks = blocksPerTimeframe[timeframe];
  const ageLimit = subMinutes(
    new Date(),
    Leaderboards.timeframeMinutesMap[timeframe],
  );
  const { left: blocksToKeep, right: blocksToRemove } = pipe(
    includedBlocks,
    A.partition((block) => isAfter(ageLimit, block.minedAt)),
  );

  if (blocksToRemove.length === 0) {
    // All blocks are young enough.
    return T.of(undefined);
  }

  const blocksToRemoveStr = blocksToRemove
    .map((block) => block.number)
    .join(",");
  Log.debug(
    `some blocks are too old in ${timeframe} timeframe, removing ${blocksToRemoveStr}`,
  );
  blocksPerTimeframe[timeframe] = blocksToKeep;
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

const rollbackToBeforeTimeframe = (
  timeframe: LimitedTimeframe,
  blockNumber: number,
  baseFeesToRemove: ContractBaseFees,
): void => {
  const includedBlocks = blocksPerTimeframe[timeframe];
  const indexOfBlockToRollbackToBefore = includedBlocks.findIndex(
    (block) => block.number === blockNumber,
  );

  if (indexOfBlockToRollbackToBefore === -1) {
    Log.warn(
      `received rollback but no blocks in timeframe ${timeframe} matched block number: ${blockNumber}, doing nothing`,
    );
    return undefined;
  }

  blocksPerTimeframe[timeframe] = includedBlocks.slice(
    0,
    indexOfBlockToRollbackToBefore,
  );
  removeFromRunningSums(timeframe, baseFeesToRemove);
  return undefined;
};

export const rollbackToBefore = (
  blockNumber: number,
  baseFeesToRemove: ContractBaseFees,
): void => {
  rollbackToBeforeTimeframe("5m", blockNumber, baseFeesToRemove);
  rollbackToBeforeTimeframe("1h", blockNumber, baseFeesToRemove);
  rollbackToBeforeTimeframe("24h", blockNumber, baseFeesToRemove);
  rollbackToBeforeTimeframe("7d", blockNumber, baseFeesToRemove);
  rollbackToBeforeTimeframe("30d", blockNumber, baseFeesToRemove);
};

export const removeExpiredBlocksFromSumsForAllTimeframes = (): T.Task<void> => {
  return pipe(
    seqTParT(
      removeExpiredBlocksFromSums("5m"),
      removeExpiredBlocksFromSums("1h"),
      removeExpiredBlocksFromSums("24h"),
      removeExpiredBlocksFromSums("7d"),
      removeExpiredBlocksFromSums("30d"),
    ),
    T.map(() => undefined),
  );
};

type ContractRow = {
  address: string;
  name: string;
  isBot: boolean;
  imageUrl: string | null;
  twitterHandle: string | null;
  category: string | null;
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
    A.takeLeft(100),
    A.map(([address]) => address),
  );

  return pipe(
    () =>
      sql<ContractRow[]>`
        SELECT address, name, is_bot, image_url, twitter_handle, category
        FROM contracts
        WHERE address IN (${topAddresses})
      `,
    T.map(
      A.map((row) => ({
        contractAddress: row.address,
        name: row.name,
        isBot: row.isBot,
        baseFees: contractSums.get(row.address)!,
        imageUrl: row.imageUrl,
        twitterHandle: row.twitterHandle,
        category: row.category,
      })),
    ),
  );
};

const calcLeaderboardForLimitedTimeframe = (
  timeframe: LimitedTimeframe,
): T.Task<LeaderboardEntry[]> => {
  return pipe(
    seqTParT(
      pipe(
        getTopBaseFeeContracts(timeframe),
        T.chain(Leaderboards.extendRowsWithFamDetails),
      ),
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
  seqSParT({
    "5m": calcLeaderboardForLimitedTimeframe("5m"),
    "1h": calcLeaderboardForLimitedTimeframe("1h"),
    "24h": calcLeaderboardForLimitedTimeframe("24h"),
    "7d": calcLeaderboardForLimitedTimeframe("7d"),
    "30d": calcLeaderboardForLimitedTimeframe("30d"),
  });
