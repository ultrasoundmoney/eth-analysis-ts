import * as DateFns from "date-fns";
import { performance } from "perf_hooks";
import { BlockDb } from "./blocks.js";
import { sql } from "./db.js";
import { A, O, Ord, pipe, RA, T, TAlt } from "./fp.js";
import * as Leaderboards from "./leaderboards.js";
import {
  ContractBaseFeesNext,
  ContractBaseFeesRow,
  ContractBaseFeeSums,
  ContractSums,
  LeaderboardEntry,
  LeaderboardRow,
} from "./leaderboards.js";
import * as Log from "./log.js";
import * as TimeFrame from "./time_frame.js";
import { LimitedTimeFrame } from "./time_frame.js";

type BlockForTotal = { number: number; minedAt: Date };

type BlocksPerTimeframe = Record<LimitedTimeFrame, BlockForTotal[]>;

type ContractSumsPerTimeframe = Record<LimitedTimeFrame, ContractSums>;

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
  timeframe: LimitedTimeFrame,
): T.Task<BlockForTotal[]> => {
  const minutes = Leaderboards.timeframeMinutesMap[timeframe];
  return () =>
    sql<BlockForTotal[]>`
      SELECT number, mined_at FROM blocks
      WHERE mined_at >= NOW() - interval '${sql(String(minutes))} minutes'
      ORDER BY number ASC
    `;
};

const getBaseFeesForRange = (
  from: number,
  upToIncluding: number,
): T.Task<ContractBaseFeesNext> =>
  pipe(
    () =>
      sql<ContractBaseFeesRow[]>`
        SELECT
          contract_address,
          SUM(base_fees) AS base_fees,
          SUM(base_fees * eth_price / 1e18) AS base_fees_usd
        FROM contract_base_fees
        JOIN blocks ON blocks.number = block_number
        WHERE block_number >= ${from}
        AND block_number <= ${upToIncluding}
        GROUP BY contract_address
    `,
    T.map(Leaderboards.collectInMap),
  );

const addToSums = (
  contractSums: ContractSums,
  baseFeesToAdd: ContractSums,
): ContractSums =>
  pipe(
    Array.from(baseFeesToAdd.entries()),
    A.reduce(contractSums, (sums, [address, feesToAdd]) => {
      const currentFees = sums.get(address) || 0;
      return sums.set(address, currentFees + feesToAdd);
    }),
  );

const subtractFromSums = (
  contractSums: ContractSums,
  baseFeesToRemove: ContractSums,
): ContractSums =>
  pipe(
    Array.from(baseFeesToRemove.entries()),
    A.reduce(contractSums, (sums, [address, feesToRemove]) => {
      const currentFees = sums.get(address);
      if (currentFees === undefined) {
        throw new Error("tried to remove base fees from a non-existing sum");
      }
      return sums.set(address, currentFees - feesToRemove);
    }),
  );

const blockForTotalOrd: Ord<BlockForTotal> = {
  equals: (x, y) => x.number === y.number,
  compare: (x, y) => (x.number < y.number ? -1 : x.number === y.number ? 0 : 1),
};

export const addAllBlocksForAllTimeframes = (): T.Task<void> =>
  pipe(
    pipe(
      TimeFrame.limitedTimeFrames,
      RA.map((timeframe) => {
        Log.debug(`init leaderboard limited time frame ${timeframe}`);

        const t0 = performance.now();

        return pipe(
          getBlocksForTimeframe(timeframe),
          T.chain((blocksToAdd) =>
            pipe(
              O.sequenceArray([A.head(blocksToAdd), A.last(blocksToAdd)]),
              O.match(
                () => {
                  Log.warn(
                    `init leaderboard limited time frame, zero blocks found in blocks table within now - ${timeframe}, skipping init`,
                  );
                  return T.of(undefined);
                },
                ([head, last]) =>
                  pipe(
                    getBaseFeesForRange(head.number, last.number),
                    T.map((sums) => {
                      blocksInTimeframe[timeframe] = blocksToAdd;

                      const sumsEth = Leaderboards.pickDenomination(
                        sums,
                        "eth",
                      );
                      const sumsUsd = Leaderboards.pickDenomination(
                        sums,
                        "usd",
                      );

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
                      Log.debug(
                        `loading leaderboard ${timeframe} took ${took}s`,
                      );

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
    ),
  );

export const addBlockForAllTimeframes = (
  block: BlockDb,
  baseFeesToAddEth: ContractSums,
  baseFeesToAddUsd: ContractSums,
): void => {
  TimeFrame.limitedTimeFrames.forEach((timeframe) => {
    blocksInTimeframe[timeframe] = pipe(
      blocksInTimeframe[timeframe],
      A.append({
        number: block.number,
        minedAt: block.minedAt,
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
  baseFeesToRemove: ContractBaseFeeSums,
): void => {
  TimeFrame.limitedTimeFrames.forEach((timeFrame) => {
    const includedBlocks = blocksInTimeframe[timeFrame];
    const indexOfBlockToRollbackToBefore = includedBlocks.findIndex(
      (block) => block.number === blockNumber,
    );

    if (indexOfBlockToRollbackToBefore === -1) {
      Log.warn(
        `received rollback but no blocks in timeframe ${timeFrame} matched block number: ${blockNumber}, doing nothing`,
      );
      return undefined;
    }

    blocksInTimeframe[timeFrame] = blocksInTimeframe[timeFrame] =
      includedBlocks.slice(0, indexOfBlockToRollbackToBefore);

    contractSumsPerTimeframe[timeFrame] = subtractFromSums(
      contractSumsPerTimeframe[timeFrame],
      baseFeesToRemove.eth,
    );
    contractSumsPerTimeframeUsd[timeFrame] = subtractFromSums(
      contractSumsPerTimeframeUsd[timeFrame],
      baseFeesToRemove.usd,
    );

    return undefined;
  });
};

export const removeExpiredBlocksFromSumsForAllTimeframes = (): T.Task<void> =>
  pipe(
    TimeFrame.limitedTimeFrames,
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
        `some blocks are too old in ${timeframe} timeFrame, removing ${blocksToRemoveStr}`,
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
            baseFees.eth,
          );
          contractSumsPerTimeframeUsd[timeframe] = subtractFromSums(
            contractSumsPerTimeframeUsd[timeframe],
            baseFees.usd,
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
  timeframe: LimitedTimeFrame,
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

  if (topAddresses.length === 0) {
    Log.warn(`no top addresses found for timeframe: ${timeframe}`);
    return T.of([]);
  }

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
  timeFrame: LimitedTimeFrame,
): T.Task<LeaderboardEntry[]> => {
  return pipe(
    TAlt.seqTParT(
      pipe(
        getTopBaseFeeContracts(timeFrame),
        T.chain(Leaderboards.extendRowsWithFamDetails),
      ),
      () => Leaderboards.getEthTransferFeesForTimeframe(timeFrame),
      () => Leaderboards.getContractCreationBaseFeesForTimeframe(timeFrame),
    ),
    T.map(([contractUse, ethTransfer, contractCreation]) =>
      Leaderboards.buildLeaderboard(contractUse, ethTransfer, contractCreation),
    ),
  );
};

export const calcLeaderboardForLimitedTimeframes = (): T.Task<
  Record<LimitedTimeFrame, LeaderboardEntry[]>
> =>
  TAlt.seqSParT({
    "5m": calcLeaderboardForLimitedTimeframe("5m"),
    "1h": calcLeaderboardForLimitedTimeframe("1h"),
    "24h": calcLeaderboardForLimitedTimeframe("24h"),
    "7d": calcLeaderboardForLimitedTimeframe("7d"),
    "30d": calcLeaderboardForLimitedTimeframe("30d"),
  });
