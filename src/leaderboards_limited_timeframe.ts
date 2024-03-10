import * as DateFns from "date-fns";
import _ from "lodash";
import { BlockV1, sortDesc } from "./blocks/blocks.js";
import { sql, sqlT } from "./db.js";
import { A, NEA, O, Ord, pipe, T, TAlt } from "./fp.js";
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
import * as Performance from "./performance.js";
import * as TimeFrames from "./time_frames.js";
import { LimitedTimeFrame } from "./time_frames.js";

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
        Log.error(
          "tried to remove base fees from a non-existing sum, doing nothing",
        );
        return sums;
      }
      return sums.set(address, currentFees - feesToRemove);
    }),
  );

const blockForTotalOrd = Ord.fromCompare<BlockForTotal>((x, y) =>
  x.number < y.number ? -1 : x.number === y.number ? 0 : 1,
);

const addAllBlocksForTimeFrame = (timeFrame: TimeFrames.LimitedTimeFrame) =>
  pipe(
    getBlocksForTimeframe(timeFrame),
    T.chain((blocksToAdd) =>
      pipe(
        O.sequenceArray([A.head(blocksToAdd), A.last(blocksToAdd)]),
        O.match(
          () => {
            Log.warn(
              `init leaderboard limited time frame, zero blocks found in blocks table within now - ${timeFrame}, skipping init`,
            );
            return T.of(undefined);
          },
          ([head, last]) =>
            pipe(
              getBaseFeesForRange(head.number, last.number),
              T.chainIOK((sums) => () => {
                blocksInTimeframe[timeFrame] = blocksToAdd;

                const sumsEth = Leaderboards.pickDenomination(sums, "eth");
                const sumsUsd = Leaderboards.pickDenomination(sums, "usd");

                contractSumsPerTimeframe[timeFrame] = addToSums(
                  sumsEth,
                  contractSumsPerTimeframe[timeFrame],
                );
                contractSumsPerTimeframeUsd[timeFrame] = addToSums(
                  sumsUsd,
                  contractSumsPerTimeframeUsd[timeFrame],
                );
              }),
            ),
        ),
      ),
    ),
  );

export const addAllBlocksForAllTimeframes = () =>
  pipe(
    TimeFrames.limitedTimeFrames,
    T.traverseSeqArray((timeFrame) =>
      pipe(
        addAllBlocksForTimeFrame(timeFrame),
        Performance.measureTaskPerf(`init leaderboard ${timeFrame}`),
      ),
    ),
  );

export const addBlockForAllTimeframes = (
  block: BlockV1,
  baseFeesToAddEth: ContractSums,
  baseFeesToAddUsd: ContractSums,
): void => {
  TimeFrames.limitedTimeFrames.forEach((timeframe) => {
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

const rollbackBlockForTimeFrames = (
  blockNumber: number,
  baseFeesToRemove: ContractBaseFeeSums,
): void => {
  for (const timeFrame of TimeFrames.limitedTimeFrames) {
    const includedBlocks = blocksInTimeframe[timeFrame];
    const indexOfBlockToRollbackToBefore = _.findLastIndex(
      includedBlocks,
      (block) => block.number === blockNumber,
    );

    if (indexOfBlockToRollbackToBefore === -1) {
      Log.debug(
        `received rollback but no blocks in timeframe ${timeFrame} matched block number: ${blockNumber}, doing nothing`,
      );
      return undefined;
    }

    blocksInTimeframe[timeFrame] = includedBlocks.slice(
      0,
      indexOfBlockToRollbackToBefore,
    );

    contractSumsPerTimeframe[timeFrame] = subtractFromSums(
      contractSumsPerTimeframe[timeFrame],
      baseFeesToRemove.eth,
    );
    contractSumsPerTimeframeUsd[timeFrame] = subtractFromSums(
      contractSumsPerTimeframeUsd[timeFrame],
      baseFeesToRemove.usd,
    );
  }
};

export const rollbackBlocks = (blocks: NEA.NonEmptyArray<BlockV1>) =>
  pipe(
    blocks,
    NEA.sort(sortDesc),
    T.traverseSeqArray((block) =>
      pipe(
        Leaderboards.getRangeBaseFees(block.number, block.number),
        T.chain((sumsToRollback) =>
          T.fromIO(() =>
            rollbackBlockForTimeFrames(block.number, sumsToRollback),
          ),
        ),
      ),
    ),
  );

const removeExpiredBlocks = (timeFrame: LimitedTimeFrame) => {
  const ageLimit = DateFns.subMinutes(
    new Date(),
    Leaderboards.timeframeMinutesMap[timeFrame],
  );
  const { expired, valid } = findExpiredBlocks(
    ageLimit,
    blocksInTimeframe[timeFrame],
  );

  if (expired.length === 0) {
    Log.debug(`no expired blocks, nothing to do for time frame ${timeFrame}`);
    return T.of(undefined);
  }

  const blocksToRemoveStr = expired.map((block) => block.number).join(", ");

  Log.debug(
    `some blocks are too old in ${timeFrame} time frame, removing ${blocksToRemoveStr}`,
  );

  blocksInTimeframe[timeFrame] = valid;

  return pipe(
    Leaderboards.getRangeBaseFees(
      expired[0].number,
      expired[expired.length - 1].number,
    ),
    T.chainIOK((baseFees) => () => {
      contractSumsPerTimeframe[timeFrame] = subtractFromSums(
        contractSumsPerTimeframe[timeFrame],
        baseFees.eth,
      );
      contractSumsPerTimeframeUsd[timeFrame] = subtractFromSums(
        contractSumsPerTimeframeUsd[timeFrame],
        baseFees.usd,
      );
    }),
  );
};

export const removeExpiredBlocksFromSumsForAllTimeframes = (): T.Task<void> =>
  pipe(
    TimeFrames.limitedTimeFrames,
    T.traverseArray(removeExpiredBlocks),
    TAlt.concatAllVoid,
  );

type ContractRow = {
  address: string;
  category: string | null;
  imageUrl: string | null;
  isBot: boolean;
  name: string;
  twitterBio: string | null;
  twitterHandle: string | null;
  twitterName: string | null;
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
    sqlT<ContractRow[]>`
      SELECT
        address,
        category,
        image_url,
        is_bot,
        name,
        twitter_description AS twitter_bio,
        twitter_handle,
        twitter_name
      FROM contracts
      WHERE address IN (${topAddresses})
    `,
    T.map(
      A.map((row) => ({
        baseFees: contractSums.get(row.address)!,
        baseFeesUsd: contractSumsUsd.get(row.address)!,
        category: row.category,
        contractAddress: row.address,
        detail: pipe(
          row.name,
          O.fromNullable,
          O.map((name) => name.split(":")[1]),
          O.map(O.fromNullable),
          O.flatten,
          O.map((detail) => detail.trimStart()),
          O.toNullable,
        ),
        imageUrl: row.imageUrl,
        isBot: row.isBot,
        name: row.name,
        twitterBio: row.twitterBio,
        twitterHandle: row.twitterHandle,
        twitterName: row.twitterName,
      })),
    ),
  );
};

const calcLeaderboardForLimitedTimeframe = (
  timeFrame: LimitedTimeFrame,
): T.Task<LeaderboardEntry[]> =>
  pipe(
    T.Do,
    T.bind("topBaseFeeContracts", () =>
      pipe(
        getTopBaseFeeContracts(timeFrame),
        Performance.measureTaskPerf(
          `    get ranked contracts for time frame ${timeFrame}`,
        ),
        T.chain(Leaderboards.extendRowsWithTwitterDetails),
        Performance.measureTaskPerf(
          `    add twitter details for time frame ${timeFrame}`,
        ),
      ),
    ),
    T.bind("ethTransfer", () =>
      pipe(
        () => Leaderboards.getEthTransferFeesForTimeframe(timeFrame),
        Performance.measureTaskPerf(
          `    add eth transfer fees for time frame ${timeFrame}`,
        ),
      ),
    ),
    T.bind("contractCreation", () =>
      pipe(
        () => Leaderboards.getContractCreationBaseFeesForTimeframe(timeFrame),
        Performance.measureTaskPerf(
          `    add contract creation fees for time frame ${timeFrame}`,
        ),
      ),
    ),
    T.bind("blobFees", () =>
      pipe(
        () => Leaderboards.getBlobBaseFeesForTimeframe(timeFrame),
        Performance.measureTaskPerf(
          `    add blob fees for time frame ${timeFrame}`,
        ),
      ),
    ),
    T.map(({ topBaseFeeContracts, ethTransfer, contractCreation, blobFees }) =>
      Leaderboards.buildLeaderboard(
        topBaseFeeContracts,
        ethTransfer,
        contractCreation,
        blobFees,
      ),
    ),
  );

export const calcLeaderboardForLimitedTimeframes = (): T.Task<
  Record<LimitedTimeFrame, LeaderboardEntry[]>
> =>
  TAlt.seqSSeq({
    "5m": calcLeaderboardForLimitedTimeframe("5m"),
    "1h": calcLeaderboardForLimitedTimeframe("1h"),
    "24h": calcLeaderboardForLimitedTimeframe("24h"),
    "7d": calcLeaderboardForLimitedTimeframe("7d"),
    "30d": calcLeaderboardForLimitedTimeframe("30d"),
  });
