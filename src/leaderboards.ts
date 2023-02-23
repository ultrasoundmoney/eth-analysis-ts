import * as DateFns from "date-fns";
import * as Arr from "fp-ts/lib/Array.js";
import * as Blocks from "./blocks/blocks.js";
import * as Log from "./log.js";
import * as Performance from "./performance.js";
import * as Task from "fp-ts/lib/Task.js";
import * as TimeFrames from "./time_frames.js";
import { sql, sqlT } from "./db.js";
import * as FamService from "./fam_service.js";
import { TwitterDetails } from "./fam_service.js";
import { NEA, O, Ord, pipe, T, TAlt, TE } from "./fp.js";
import { FixedDurationTimeFrame, TimeFrame } from "./time_frames.js";
import _ from "lodash";

type BlockForTotal = { number: number; minedAt: Date };

type BlocksPerTimeframe = Record<TimeFrame, BlockForTotal[]>;

type ContractSumsPerTimeframe = Record<TimeFrame, ContractSums>;

// These are the blocks that make up the base fee sums for our limited timeframes.
const blocksInTimeframe: BlocksPerTimeframe = {
  "5m": [],
  "1h": [],
  "24h": [],
  "7d": [],
  "30d": [],
  since_burn: [],
  since_merge: [],
};

// These are the base fee sums per contract, per timeframe.
const contractSumsPerTimeframe: ContractSumsPerTimeframe = {
  "5m": new Map(),
  "1h": new Map(),
  "24h": new Map(),
  "7d": new Map(),
  "30d": new Map(),
  since_burn: new Map(),
  since_merge: new Map(),
};

const contractSumsPerTimeframeUsd: ContractSumsPerTimeframe = {
  "5m": new Map(),
  "1h": new Map(),
  "24h": new Map(),
  "7d": new Map(),
  "30d": new Map(),
  since_burn: new Map(),
  since_merge: new Map(),
};

type SyncStatus = "unknown" | "in-sync" | "out-of-sync";
let syncStatus: SyncStatus = "unknown";

// TODO: Move leaderboards... into a folder.
// TODO: Rewrite using pure DB like burn records.

export type LeaderboardRow = {
  baseFees: number;
  baseFeesUsd: number;
  category: string | null;
  contractAddress: string;
  detail: string | null;
  imageUrl: string | null;
  isBot: boolean;
  name: string | null;
  twitterBio: string | null;
  twitterHandle: string | null;
  twitterName: string | null;
};

export type LeaderboardRowWithTwitterDetails = {
  baseFees: BaseFees;
  category: string | null;
  contractAddress: string;
  detail: string | null;
  famFollowerCount: number | undefined;
  followerCount: number | undefined;
  imageUrl: string | null;
  isBot: boolean;
  name: string | null;
  twitterBio: string | undefined;
  twitterHandle: string | undefined;
  twitterLinks: FamService.Linkables | undefined;
  twitterName: string | undefined;
};

type ContractEntry = {
  address: string;
  category: string | null;
  detail: string | null;
  famFollowerCount: number | undefined;
  fees: number;
  feesUsd: number;
  followerCount: number | undefined;
  /**
   * @deprecated
   */
  id: string;
  image: string | null;
  isBot: boolean;
  name: string | null;
  type: "contract";
  twitterBio: string | undefined;
  twitterHandle: string | undefined;
  twitterLinks: FamService.Linkables | undefined;
  twitterUrl: string | undefined;
};

type EthTransfersEntry = {
  type: "eth-transfers";
  name: string;
  fees: number;
  feesUsd: number;
  /**
   * @deprecated
   */
  id: string;
};

type ContractCreationsEntry = {
  type: "contract-creations";
  name: string;
  fees: number;
  feesUsd: number;
  /**
   * @deprecated
   */
  id: string;
};

// Name is undefined because we don't always know the name for a contract. Image is undefined because we don't always have an image for a contract. Address is undefined because base fees paid for ETH transfers are shared between many addresses.
export type LeaderboardEntry =
  | ContractEntry
  | EthTransfersEntry
  | ContractCreationsEntry;

export type LeaderboardEntries = {
  leaderboard5m: LeaderboardEntry[];
  leaderboard1h: LeaderboardEntry[];
  leaderboard24h: LeaderboardEntry[];
  leaderboard7d: LeaderboardEntry[];
  leaderboard30d: LeaderboardEntry[];
  leaderboardSinceMerge: LeaderboardEntry[];
  leaderboardSinceBurn: LeaderboardEntry[];
};

export type ContractBaseFees = Map<string, number>;
export type ContractBaseFeesRow = {
  contractAddress: string;
  baseFees: number;
  baseFeesUsd: number;
};

export type ContractBaseFeesNext = Map<string, { eth: number; usd: number }>;

export type ContractBaseFeeSums = { eth: ContractSums; usd: ContractSums };

export const collectInMap = (rows: ContractBaseFeesRow[]) =>
  pipe(
    rows,
    Arr.reduce(new Map() as ContractBaseFeesNext, (map, row) => {
      return map.set(row.contractAddress, {
        eth: row.baseFees,
        usd: row.baseFeesUsd,
      });
    }),
  );

export const getRangeBaseFees = (
  from: number,
  upToIncluding: number,
): T.Task<ContractBaseFeeSums> =>
  pipe(
    () => sql<ContractBaseFeesRow[]>`
      SELECT
        contract_address,
        SUM(base_fees) AS base_fees,
        SUM(base_fees * eth_price / 1e18) AS base_fees_usd
      FROM contract_base_fees
      JOIN blocks ON blocks.number = block_number
      WHERE block_number >= ${from}
      AND block_number <= ${upToIncluding}
      GROUP BY (contract_address)
  `,
    T.map(
      Arr.reduce({ eth: new Map(), usd: new Map() }, (sums, row) => ({
        eth: sums.eth.set(row.contractAddress, row.baseFees),
        usd: sums.usd.set(row.contractAddress, row.baseFeesUsd),
      })),
    ),
  );

export const mergeBaseFees = (
  baseFeeRowsList: ContractBaseFees[],
): ContractBaseFees => {
  return pipe(
    baseFeeRowsList,
    Arr.reduce(new Map(), (sumMap, [address, baseFees]) => {
      const sum = sumMap.get(address) ?? 0;
      return sumMap.set(address, sum + baseFees);
    }),
  );
};

export const timeframeMinutesMap: Record<FixedDurationTimeFrame, number> = {
  "5m": 5,
  "1h": 1 * 60,
  "24h": 24 * 60,
  "7d": 7 * 24 * 60,
  "30d": 30 * 24 * 60,
};

export const getEthTransferFeesForTimeframe = async (
  timeframe: TimeFrame,
): Promise<BaseFees> => {
  if (timeframe === "since_burn") {
    const rows = await sql<{ eth: number; usd: number }[]>`
      SELECT
        SUM(eth_transfer_sum) AS eth,
        SUM(eth_transfer_sum * eth_price / 1e18) AS usd
      FROM blocks
    `;
    return { eth: rows[0]?.eth ?? 0, usd: rows[0]?.usd ?? 0 };
  }

  if (timeframe === "since_merge") {
    const rows = await sql<{ eth: number; usd: number }[]>`
      SELECT
        SUM(eth_transfer_sum) AS eth,
        SUM(eth_transfer_sum * eth_price / 1e18) AS usd
      FROM blocks
      WHERE number >= ${Blocks.mergeBlockNumber}
    `;
    return { eth: rows[0]?.eth ?? 0, usd: rows[0]?.usd ?? 0 };
  }

  const minutes = timeframeMinutesMap[timeframe];
  const rows = await sql<{ eth: number; usd: number }[]>`
      SELECT
        SUM(eth_transfer_sum) AS eth,
        SUM(eth_transfer_sum * eth_price / 1e18) AS usd
      FROM blocks
      WHERE mined_at >= NOW() - interval '${sql(String(minutes))} minutes'
  `;
  return { eth: rows[0]?.eth ?? 0, usd: rows[0]?.usd ?? 0 };
};

export const getContractCreationBaseFeesForTimeframe = async (
  timeframe: TimeFrame,
): Promise<BaseFees> => {
  if (timeframe === "since_burn") {
    const rows = await sql<{ eth: number; usd: number }[]>`
      SELECT
        SUM(contract_creation_sum) AS eth,
        SUM(contract_creation_sum * eth_price / 1e18) AS usd
      FROM blocks
    `;
    return { eth: rows[0]?.eth ?? 0, usd: rows[0]?.usd ?? 0 };
  }

  if (timeframe === "since_merge") {
    const rows = await sql<{ eth: number; usd: number }[]>`
      SELECT
        SUM(contract_creation_sum) AS eth,
        SUM(contract_creation_sum * eth_price / 1e18) AS usd
      FROM blocks
      WHERE number >= ${Blocks.mergeBlockNumber}
    `;
    return { eth: rows[0]?.eth ?? 0, usd: rows[0]?.usd ?? 0 };
  }

  const minutes = timeframeMinutesMap[timeframe];
  const rows = await sql<{ eth: number; usd: number }[]>`
      SELECT
        SUM(contract_creation_sum) AS eth,
        SUM(contract_creation_sum * eth_price / 1e18) AS usd
      FROM blocks
      WHERE mined_at >= NOW() - interval '${sql(String(minutes))} minutes'
  `;
  return { eth: rows[0]?.eth ?? 0, usd: rows[0]?.usd ?? 0 };
};

type BaseFees = {
  eth: number;
  usd: number;
};

export const buildLeaderboard = (
  contractRows: LeaderboardRowWithTwitterDetails[],
  ethTransferBaseFees: BaseFees,
  contractCreationBaseFees: BaseFees,
): LeaderboardEntry[] => {
  const contractEntries: ContractEntry[] = contractRows.map((row) => ({
    address: row.contractAddress,
    category: row.category,
    detail: row.detail,
    famFollowerCount: row.famFollowerCount,
    fees: Number(row.baseFees.eth),
    feesUsd: Number(row.baseFees.usd),
    followerCount: row.followerCount,
    id: row.contractAddress,
    image: row.imageUrl,
    isBot: row.isBot,
    name: row.name,
    twitterBio: row.twitterBio,
    twitterHandle: row.twitterHandle,
    twitterLinks: row.twitterLinks,
    twitterName: row.twitterName,
    twitterUrl: pipe(
      row.twitterHandle,
      O.fromNullable,
      O.map((handle) => `https://twitter.com/${handle}`),
      O.toUndefined,
    ),
    type: "contract",
  }));

  const contractCreationEntry: ContractCreationsEntry = {
    fees: contractCreationBaseFees.eth,
    feesUsd: contractCreationBaseFees.usd,
    id: "contract-creations",
    name: "new contracts",
    type: "contract-creations",
  };

  const ethTransfersEntry: EthTransfersEntry = {
    fees: ethTransferBaseFees.eth,
    feesUsd: ethTransferBaseFees.usd,
    id: "eth-transfers",
    name: "ETH transfers",
    type: "eth-transfers",
  };

  return pipe(
    [...contractEntries, ethTransfersEntry, contractCreationEntry],
    Arr.sort<LeaderboardEntry>({
      compare: (first, second) =>
        first.fees === second.fees ? 0 : first.fees > second.fees ? -1 : 1,
      equals: (first, second) => first.fees === second.fees,
    }),
    Arr.takeLeft(100),
  );
};

const buildRanking = (
  row: LeaderboardRow,
): LeaderboardRowWithTwitterDetails => ({
  ...row,
  baseFees: {
    eth: row.baseFees,
    usd: row.baseFeesUsd,
  },
  famFollowerCount: undefined,
  followerCount: undefined,
  twitterBio: undefined,
  twitterHandle: row.twitterHandle ?? undefined,
  twitterLinks: undefined,
  twitterName: row.twitterName ?? undefined,
});

const buildRankingWithTwitterDetails = (
  row: LeaderboardRow,
  twitterDetails: TwitterDetails,
): LeaderboardRowWithTwitterDetails => ({
  ...row,
  baseFees: {
    eth: row.baseFees,
    usd: row.baseFeesUsd,
  },
  famFollowerCount: twitterDetails.famFollowerCount,
  followerCount: twitterDetails.followerCount,
  twitterBio: twitterDetails.bio,
  twitterHandle: row.twitterHandle ?? undefined,
  twitterLinks: twitterDetails.links,
  twitterName: row.twitterName ?? undefined,
});

export const extendRowsWithTwitterDetails = (
  leaderboardRows: LeaderboardRow[],
): T.Task<LeaderboardRowWithTwitterDetails[]> =>
  pipe(
    leaderboardRows,
    Arr.map((row) => row.twitterHandle),
    Arr.map(O.fromNullable),
    Arr.compact,
    (list) => new Set(list),
    (set) => Array.from(set),
    NEA.fromArray,
    O.matchW(
      () => T.of([]),
      (handles) =>
        pipe(
          FamService.getDetailsByHandles(handles),
          TE.getOrElseW((e) => {
            Log.error("failed to get fam details", e);
            return T.of([]);
          }),
        ),
    ),
    T.map(
      Arr.reduce(new Map<string, TwitterDetails>(), (map, details) =>
        map.set(details.handle.toLowerCase(), details),
      ),
    ),
    T.map((twitterDetailsMap) =>
      pipe(
        leaderboardRows,
        Arr.map((row) => {
          if (row.twitterHandle === null) {
            return buildRanking(row);
          }

          const twitterDetails = twitterDetailsMap.get(
            row.twitterHandle.toLowerCase(),
          );
          if (twitterDetails === undefined) {
            // Fam service did not have details for this twitter handle.
            return buildRanking(row);
          }

          return buildRankingWithTwitterDetails(row, twitterDetails);
        }),
      ),
    ),
  );

export type ContractAddress = string;
export type ContractSums = Map<ContractAddress, number>;

export const pickDenomination = (
  sums: ContractBaseFeesNext,
  denomination: "eth" | "usd",
): ContractSums =>
  pipe(
    Array.from(sums.entries()),
    Arr.map(
      ([address, price]) => [address, price[denomination]] as [string, number],
    ),
    (entries) => new Map(entries),
  );


export const getSyncStatus = (): SyncStatus => syncStatus;
export const setSyncStatus = (newSyncStatus: SyncStatus): void => {
  syncStatus = newSyncStatus;
};

const getBlocksForTimeframe = (
  timeframe: TimeFrame,
): T.Task<BlockForTotal[]> => {
  if (timeframe === "since_merge") {
    return () =>
      sql<BlockForTotal[]>`
        SELECT number, mined_at FROM blocks
        WHERE number >= ${Blocks.mergeBlockNumber}
        ORDER BY number ASC
        `;
  }
  if (timeframe == "since_burn") {
    return () =>
      sql<BlockForTotal[]>`
        SELECT number, mined_at FROM blocks
        WHERE number >= ${Blocks.londonHardForkBlockNumber}
        ORDER BY number ASC
        `;
  }
  const minutes = timeframeMinutesMap[timeframe];
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
    T.map(collectInMap),
  );

const addToSums = (
  contractSums: ContractSums,
  baseFeesToAdd: ContractSums,
): ContractSums =>
  pipe(
    Array.from(baseFeesToAdd.entries()),
    Arr.reduce(contractSums, (sums, [address, feesToAdd]) => {
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
    Arr.reduce(contractSums, (sums, [address, feesToRemove]) => {
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

const addAllBlocksForTimeFrame = (timeFrame: TimeFrames.TimeFrame) =>
  pipe(
    getBlocksForTimeframe(timeFrame),
    T.chain((blocksToAdd) =>
      pipe(
        O.sequenceArray([Arr.head(blocksToAdd), Arr.last(blocksToAdd)]),
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

                const sumsEth = pickDenomination(sums, "eth");
                const sumsUsd = pickDenomination(sums, "usd");

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
    TimeFrames.timeFrames,
    T.traverseSeqArray((timeFrame) =>
      pipe(
        addAllBlocksForTimeFrame(timeFrame),
        Performance.measureTaskPerf(`init leaderboard ${timeFrame}`),
      ),
    ),
  );

export const addBlockForAllTimeframes = (
  block: Blocks.BlockV1,
  baseFeesToAddEth: ContractSums,
  baseFeesToAddUsd: ContractSums,
): void => {
  TimeFrames.timeFrames.forEach((timeframe) => {
    blocksInTimeframe[timeframe] = pipe(
      blocksInTimeframe[timeframe],
      Arr.append({
        number: block.number,
        minedAt: block.minedAt,
      }),
      Arr.sort(blockForTotalOrd),
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
    Arr.partition((block) => DateFns.isAfter(ageLimit, block.minedAt)),
  );

  return { valid, expired };
};

const rollbackBlockForTimeFrames = (
  blockNumber: number,
  baseFeesToRemove: ContractBaseFeeSums,
): void => {
  for (const timeFrame of TimeFrames.timeFrames) {
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

export const rollbackBlocks = (blocks: NEA.NonEmptyArray<Blocks.BlockV1>) =>
  pipe(
    blocks,
    NEA.sort(Blocks.sortDesc),
    T.traverseSeqArray((block) =>
      pipe(
        getRangeBaseFees(block.number, block.number),
        T.chain((sumsToRollback) =>
          T.fromIO(() =>
            rollbackBlockForTimeFrames(block.number, sumsToRollback),
          ),
        ),
      ),
    ),
  );

const removeExpiredBlocks = (timeFrame: TimeFrame) => {
  const ageLimit =
    timeFrame === "since_merge"
      ? Blocks.mergeBlockDate
      : timeFrame === "since_burn"
      ? Blocks.londonHardForkBlockDate
      : DateFns.subMinutes(
          new Date(),
          timeframeMinutesMap[timeFrame],
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
    getRangeBaseFees(
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
    TimeFrames.timeFrames,
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
  timeframe: TimeFrame,
): T.Task<LeaderboardRow[]> => {
  const contractSums = contractSumsPerTimeframe[timeframe];
  const contractSumsUsd = contractSumsPerTimeframeUsd[timeframe];
  const topAddresses = pipe(
    Array.from(contractSums.entries()),
    Arr.sort<[string, number]>({
      equals: ([, baseFeeA], [, baseFeeB]) => baseFeeA === baseFeeB,
      compare: ([, baseFeeA], [, baseFeeB]) => (baseFeeA < baseFeeB ? 1 : -1),
    }),
    Arr.takeLeft(100),
    Arr.map(([address]) => address),
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
      Arr.map((row) => ({
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

const calcLeaderboardForTimeFrame = (
  timeFrame: TimeFrame,
): T.Task<LeaderboardEntry[]> =>
  pipe(
    T.Do,
    T.bind("topBaseFeeContracts", () =>
      pipe(
        getTopBaseFeeContracts(timeFrame),
        Performance.measureTaskPerf(
          `    get ranked contracts for time frame ${timeFrame}`,
        ),
        T.chain(extendRowsWithTwitterDetails),
        Performance.measureTaskPerf(
          `    add twitter details for time frame ${timeFrame}`,
        ),
      ),
    ),
    T.bind("ethTransfer", () =>
      pipe(
        () => getEthTransferFeesForTimeframe(timeFrame),
        Performance.measureTaskPerf(
          `    add eth transfer fees for time frame ${timeFrame}`,
        ),
      ),
    ),
    T.bind("contractCreation", () =>
      pipe(
        () => getContractCreationBaseFeesForTimeframe(timeFrame),
        Performance.measureTaskPerf(
          `    add contract creation fees for time frame ${timeFrame}`,
        ),
      ),
    ),
    T.map(({ topBaseFeeContracts, ethTransfer, contractCreation }) =>
      buildLeaderboard(
        topBaseFeeContracts,
        ethTransfer,
        contractCreation,
      ),
    ),
  );

export const calcLeaderboardForTimeFrames = (): T.Task<
  Record<TimeFrame, LeaderboardEntry[]>
> =>
  TAlt.seqSSeq({
    "5m": calcLeaderboardForTimeFrame("5m"),
    "1h": calcLeaderboardForTimeFrame("1h"),
    "24h": calcLeaderboardForTimeFrame("24h"),
    "7d": calcLeaderboardForTimeFrame("7d"),
    "30d": calcLeaderboardForTimeFrame("30d"),
    "since_burn": calcLeaderboardForTimeFrame("since_burn"),
    "since_merge": calcLeaderboardForTimeFrame("since_merge"),
  });
