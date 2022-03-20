import * as A from "fp-ts/lib/Array.js";
import * as Log from "./log.js";
import { pipe } from "fp-ts/lib/function.js";
import * as T from "fp-ts/lib/Task.js";
import { sql } from "./db.js";
import * as FamService from "./fam_service.js";
import { TwitterDetails } from "./fam_service.js";
import { NEA, O, TE } from "./fp.js";
import { LimitedTimeFrame, TimeFrame } from "./time_frames.js";

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
  twitterDescription: string | null;
  twitterHandle: string | null;
  twitterName: string | null;
};

export type LeaderboardRowWithTwitterDetails = {
  baseFees: BaseFees;
  category: string | null;
  contractAddress: string;
  detail: string | null;
  famFollowerCount: number | null;
  followersCount: number | null;
  imageUrl: string | null;
  isBot: boolean;
  name: string | null;
  twitterDescription: string | null;
  twitterHandle: string | null;
  twitterName: string | null;
};

type ContractEntry = {
  address: string;
  category: string | null;
  detail: string | null;
  fees: number;
  feesUsd: number;
  /**
   * @deprecated
   */
  id: string;
  image: string | null;
  isBot: boolean;
  name: string | null;
  type: "contract";
  twitterHandle: string | null;
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
  leaderboardAll: LeaderboardEntry[];
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
    A.reduce(new Map() as ContractBaseFeesNext, (map, row) => {
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
      A.reduce({ eth: new Map(), usd: new Map() }, (sums, row) => ({
        eth: sums.eth.set(row.contractAddress, row.baseFees),
        usd: sums.usd.set(row.contractAddress, row.baseFeesUsd),
      })),
    ),
  );

export type LeaderboardsT = {
  leaderboard5m: LeaderboardRow[];
  leaderboard1h: LeaderboardRow[];
  leaderboard24h: LeaderboardRow[];
  leaderboard7d: LeaderboardRow[];
  leaderboard30d: LeaderboardRow[];
  leaderboardAll: LeaderboardRow[];
};

export const mergeBaseFees = (
  baseFeeRowsList: ContractBaseFees[],
): ContractBaseFees => {
  return pipe(
    baseFeeRowsList,
    A.reduce(new Map(), (sumMap, [address, baseFees]) => {
      const sum = sumMap.get(address) ?? 0;
      return sumMap.set(address, sum + baseFees);
    }),
  );
};

export const timeframeMinutesMap: Record<LimitedTimeFrame, number> = {
  "5m": 5,
  "1h": 1 * 60,
  "24h": 24 * 60,
  "7d": 7 * 24 * 60,
  "30d": 30 * 24 * 60,
};

export const getEthTransferFeesForTimeframe = async (
  timeframe: TimeFrame,
): Promise<BaseFees> => {
  if (timeframe === "all") {
    const rows = await sql<{ eth: number; usd: number }[]>`
      SELECT
        SUM(eth_transfer_sum) AS eth,
        SUM(eth_transfer_sum * eth_price / 1e18) AS usd
      FROM blocks
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
  if (timeframe === "all") {
    const rows = await sql<{ eth: number; usd: number }[]>`
      SELECT
        SUM(contract_creation_sum) AS eth,
        SUM(contract_creation_sum * eth_price / 1e18) AS usd
      FROM blocks
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
    followersCount: row.followersCount,
    id: row.contractAddress,
    image: row.imageUrl,
    isBot: row.isBot,
    name: row.name || row.contractAddress,
    twitterDescription: row.twitterDescription,
    twitterHandle: row.twitterHandle,
    twitterName: row.twitterName,
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
    A.sort<LeaderboardEntry>({
      compare: (first, second) =>
        first.fees === second.fees ? 0 : first.fees > second.fees ? -1 : 1,
      equals: (first, second) => first.fees === second.fees,
    }),
    A.takeLeft(100),
  );
};

export const extendRowsWithTwitterDetails = (
  leaderboardRows: LeaderboardRow[],
): T.Task<LeaderboardRowWithTwitterDetails[]> =>
  pipe(
    leaderboardRows,
    A.map((row) => row.twitterHandle),
    A.map(O.fromNullable),
    A.compact,
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
      A.reduce(new Map<string, TwitterDetails>(), (map, details) =>
        map.set(details.handle, details),
      ),
    ),
    T.map((twitterDetails) =>
      pipe(
        leaderboardRows,
        A.map((row) => {
          if (row.twitterHandle === null) {
            return {
              ...row,
              baseFees: {
                eth: row.baseFees,
                usd: row.baseFeesUsd,
              },
              followersCount: null,
              famFollowerCount: null,
            };
          }

          const detail = twitterDetails.get(row.twitterHandle);
          if (detail === undefined) {
            // Fam service did not have details for this twitter handle.
            return {
              ...row,
              baseFees: {
                eth: row.baseFees,
                usd: row.baseFeesUsd,
              },
              famFollowerCount: null,
              followersCount: null,
            };
          }

          return {
            ...row,
            baseFees: {
              eth: row.baseFees,
              usd: row.baseFeesUsd,
            },
            famFollowerCount: detail.famFollowerCount,
            followersCount: detail.followersCount,
          };
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
    A.map(
      ([address, price]) => [address, price[denomination]] as [string, number],
    ),
    (entries) => new Map(entries),
  );
