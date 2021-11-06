import * as Leaderboards from "./leaderboards.js";
import * as Log from "./log.js";
import { A, O, Ord, pipe, RA, seqSParT, seqTParT, T } from "./fp.js";
import * as DateFns from "date-fns";
import { sql } from "./db.js";
import { BlockLondon } from "./eth_node.js";
import { performance } from "perf_hooks";
import {
  ContractBaseFeesNext,
  ContractBaseFeesRow,
  ContractSums,
  LeaderboardEntry,
  LeaderboardRow,
  LimitedTimeframe,
} from "./leaderboards.js";

type BlockForTotal = { number: number; minedAt: Date };

type BlocksPerTimeframe = Record<LimitedTimeframe, BlockForTotal[]>;

type ContractSumsPerTimeframe = Record<LimitedTimeframe, ContractSums>;

// These are the blocks that make up the base fee sums for our limited timeframes.
const blocksInTimeframe: BlocksPerTimeframe = {
  "5m": [],
  "1h": [],
  "24h": [],
  "7d": [],
  "30d": [],
};

// These are the base fee sums per contract, per timeframe.
const contractSumsPerTimeframe: ContractSumsPerTimeframe = {
  "5m": new Map(),
  "1h": new Map(),
  "24h": new Map(),
  "7d": new Map(),
  "30d": new Map(),
};

const contractSumsPerTimeframeUsd: ContractSumsPerTimeframe = {
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
      ORDER BY number ASC
    `;
};

const getBaseFeesForRange = (
  from: number,
  upToIncluding: number,
): T.Task<ContractBaseFeesNext> => {
  return pipe(
    () =>
      sql<ContractBaseFeesRow[]>`
        SELECT
          contract_address,
          SUM(base_fees) AS base_fees,
          SUM(base_fees * eth_price) AS base_fees_usd
        FROM contract_base_fees
        JOIN blocks ON blocks.number = block_number
        WHERE block_number >= ${from}
        AND block_number <= ${upToIncluding}
        GROUP BY contract_address
      `,
    T.map(Leaderboards.collectInMap),
  );
};

const addToSums = (
  contractSums: ContractSums,
  baseFeesToAdd: ContractSums,
): ContractSums => {
  baseFeesToAdd.forEach((baseFees, contractAddress) => {
    const currentBaseFeeSum = contractSums.get(contractAddress) || 0;
    contractSums.set(contractAddress, currentBaseFeeSum + baseFees);
  });
  return contractSums;
};

const subtractFromSums = (
  contractSums: ContractSums,
  baseFeesToRemove: ContractSums,
): ContractSums => {
  for (const [contractAddress, baseFees] of baseFeesToRemove.entries()) {
    const currentBaseFeeSum = contractSums.get(contractAddress);
    if (currentBaseFeeSum === undefined) {
      throw new Error("tried to remove base fees from a non-existing sum");
    }
    contractSums.set(contractAddress, currentBaseFeeSum - baseFees);
  }
  return contractSums;
};

const blockForTotalOrd: Ord<BlockForTotal> = {
  equals: (x, y) => x.number === y.number,
  compare: (x, y) => (x.number < y.number ? -1 : 1),
};

export const addAllBlocksForAllTimeframes = (
  upToIncluding: number,
): T.Task<void> =>
  pipe(
    Leaderboards.limitedTimeframes,
    RA.map((timeframe) => {
      const t0 = performance.now();
      Log.debug(`loading sums for ${timeframe}`);
      return pipe(
        getBlocksForTimeframe(timeframe, upToIncluding),
        T.chain((blocksToAdd) =>
          pipe(
            O.sequenceArray([A.head(blocksToAdd), A.last(blocksToAdd)]),
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
                    blocksInTimeframe[timeframe] = pipe(
                      blocksInTimeframe[timeframe],
                      A.concat(blocksToAdd),
                      A.sort(blockForTotalOrd),
                    );

                    const sumsEth = Leaderboards.pickDenomination(sums, "eth");
                    const sumsUsd = Leaderboards.pickDenomination(sums, "usd");

                    contractSumsPerTimeframe[timeframe] = addToSums(
                      sumsEth,
                      contractSumsPerTimeframe[timeframe],
                    );
                    contractSumsPerTimeframeUsd[timeframe] = addToSums(
                      sumsUsd,
                      contractSumsPerTimeframeUsd[timeframe],
                    );

                    const t1 = performance.now();
                    const took = Number((t1 - t0) / 1000).toFixed(2);
                    Log.debug(`loading leaderboard ${timeframe} took ${took}s`);

                    return undefined;
                  }),
                ),
            ),
          ),
        ),
      );
    }),
    T.sequenceArray,
    T.map(() => undefined),
  );

export const addBlockForAllTimeframes = (
  block: BlockLondon,
  baseFeesToAdd: ContractBaseFeesNext,
): void => {
  const baseFeesToAddEth = Leaderboards.pickDenomination(baseFeesToAdd, "eth");
  const baseFeesToAddUsd = Leaderboards.pickDenomination(baseFeesToAdd, "usd");

  Leaderboards.limitedTimeframes.forEach((timeframe) => {
    blocksInTimeframe[timeframe] = pipe(
      blocksInTimeframe[timeframe],
      A.append({
        number: block.number,
        minedAt: DateFns.fromUnixTime(block.timestamp),
      }),
      A.sort(blockForTotalOrd),
    );
    addToSums(contractSumsPerTimeframe[timeframe], baseFeesToAddEth);
    addToSums(contractSumsPerTimeframeUsd[timeframe], baseFeesToAddUsd);
  });
};

const findExpiredBlocks = (
  ageLimit: Date,
  includedBlocks: BlockForTotal[],
): { valid: BlockForTotal[]; expired: BlockForTotal[] } => {
  const { left: valid, right: expired } = pipe(
    includedBlocks,
    A.partition((block) => DateFns.isAfter(ageLimit, block.minedAt)),
  );

  return { valid, expired };
};

export const rollbackToBefore = (
  blockNumber: number,
  baseFeesToRemove: ContractBaseFeesNext,
): void => {
  Leaderboards.limitedTimeframes.forEach((timeframe) => {
    const includedBlocks = blocksInTimeframe[timeframe];
    const indexOfBlockToRollbackToBefore = includedBlocks.findIndex(
      (block) => block.number === blockNumber,
    );

    if (indexOfBlockToRollbackToBefore === -1) {
      Log.warn(
        `received rollback but no blocks in timeframe ${timeframe} matched block number: ${blockNumber}, doing nothing`,
      );
      return undefined;
    }

    blocksInTimeframe[timeframe] = blocksInTimeframe[timeframe] =
      includedBlocks.slice(0, indexOfBlockToRollbackToBefore);

    contractSumsPerTimeframe[timeframe] = subtractFromSums(
      contractSumsPerTimeframe[timeframe],
      Leaderboards.pickDenomination(baseFeesToRemove, "eth"),
    );
    contractSumsPerTimeframeUsd[timeframe] = subtractFromSums(
      contractSumsPerTimeframeUsd[timeframe],
      Leaderboards.pickDenomination(baseFeesToRemove, "usd"),
    );

    return undefined;
  });
};

export const removeExpiredBlocksFromSumsForAllTimeframes = (): T.Task<void> =>
  pipe(
    Leaderboards.limitedTimeframes,
    RA.map((timeframe) => {
      const ageLimit = DateFns.subMinutes(
        new Date(),
        Leaderboards.timeframeMinutesMap[timeframe],
      );
      const { expired, valid } = findExpiredBlocks(
        ageLimit,
        blocksInTimeframe[timeframe],
      );

      if (expired.length === 0) {
        Log.debug(
          `no expired blocks, nothing to do for timeframe ${timeframe}`,
        );
        return T.of(undefined);
      }

      const blocksToRemoveStr = expired.map((block) => block.number).join(",");

      Log.debug(
        `some blocks are too old in ${timeframe} timeframe, removing ${blocksToRemoveStr}`,
      );

      blocksInTimeframe[timeframe] = valid;

      return pipe(
        Leaderboards.getRangeBaseFees(
          expired[0].number,
          expired[expired.length - 1].number,
        ),
        T.chainIOK((baseFees) => () => {
          contractSumsPerTimeframe[timeframe] = subtractFromSums(
            contractSumsPerTimeframe[timeframe],
            Leaderboards.pickDenomination(baseFees, "eth"),
          );
          contractSumsPerTimeframeUsd[timeframe] = subtractFromSums(
            contractSumsPerTimeframeUsd[timeframe],
            Leaderboards.pickDenomination(baseFees, "usd"),
          );
        }),
      );
    }),
    T.sequenceArray,
    T.map(() => undefined),
  );

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
  const contractSumsUsd = contractSumsPerTimeframeUsd[timeframe];
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
        SELECT
          address,
          category,
          image_url,
          is_bot,
          name,
          twitter_handle
        FROM contracts
        WHERE address IN (${topAddresses})
      `,
    T.map(
      A.map((row) => ({
        baseFees: contractSums.get(row.address)!,
        baseFeesUsd: contractSumsUsd.get(row.address)!,
        category: row.category,
        contractAddress: row.address,
        imageUrl: row.imageUrl,
        isBot: row.isBot,
        name: row.name,
        twitterHandle: row.twitterHandle,
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
