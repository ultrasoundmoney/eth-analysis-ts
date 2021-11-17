import * as A from "fp-ts/lib/Array.js";
import { pipe } from "fp-ts/lib/function.js";
import * as T from "fp-ts/lib/Task.js";
import { sql } from "./db.js";
import * as FamService from "./fam_service.js";
import { FamDetails } from "./fam_service.js";
import { O } from "./fp.js";
import { LimitedTimeframe, Timeframe } from "./timeframe.js";

export type LeaderboardRow = {
  contractAddress: string;
  name: string;
  isBot: boolean;
  baseFees: number;
  baseFeesUsd: number;
  imageUrl: string | null;
  twitterHandle: string | null;
  category: string | null;
};

export type LeaderboardRowWithFamDetails = {
  baseFees: BaseFees;
  bio: string | null;
  category: string | null;
  contractAddress: string;
  famFollowerCount: number | null;
  followersCount: number | null;
  imageUrl: string | null;
  isBot: boolean;
  name: string;
  twitterHandle: string | null;
  twitterName: string | null;
};

type ContractEntry = {
  type: "contract";
  name: string | null;
  image: string | null;
  fees: number;
  feesUsd: number;
  address: string;
  category: string | null;
  isBot: boolean;
  twitterHandle: string | null;
  /* deprecated */
  id: string;
};

type EthTransfersEntry = {
  type: "eth-transfers";
  name: string;
  fees: number;
  feesUsd: number;
  /* deprecated */
  id: string;
};

type ContractCreationsEntry = {
  type: "contract-creations";
  name: string;
  fees: number;
  feesUsd: number;
  /* deprecated */
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
    A.reduce(new Map(), (map, row) => {
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
        SUM(base_fees * eth_price / POWER(10, 18)) AS base_fees_usd
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

export const timeframeMinutesMap: Record<LimitedTimeframe, number> = {
  "5m": 5,
  "1h": 1 * 60,
  "24h": 24 * 60,
  "7d": 7 * 24 * 60,
  "30d": 30 * 24 * 60,
};

export const getEthTransferFeesForTimeframe = async (
  timeframe: Timeframe,
): Promise<BaseFees> => {
  if (timeframe === "all") {
    const rows = await sql<{ eth: number; usd: number }[]>`
      SELECT
        SUM(eth_transfer_sum) AS eth,
        SUM(eth_transfer_sum * eth_price / POWER(10, 18)) AS usd
      FROM blocks
    `;
    return { eth: rows[0]?.eth ?? 0, usd: rows[0]?.usd ?? 0 };
  }

  const minutes = timeframeMinutesMap[timeframe];
  const rows = await sql<{ eth: number; usd: number }[]>`
      SELECT
        SUM(eth_transfer_sum) AS eth,
        SUM(eth_transfer_sum * eth_price / POWER(10, 18)) AS usd
      FROM blocks
      WHERE mined_at >= NOW() - interval '${sql(String(minutes))} minutes'
  `;
  return { eth: rows[0]?.eth ?? 0, usd: rows[0]?.usd ?? 0 };
};

export const getContractCreationBaseFeesForTimeframe = async (
  timeframe: Timeframe,
): Promise<BaseFees> => {
  if (timeframe === "all") {
    const rows = await sql<{ eth: number; usd: number }[]>`
      SELECT
        SUM(contract_creation_sum) AS eth,
        SUM(contract_creation_sum * eth_price / POWER(10, 18)) AS usd
      FROM blocks
    `;
    return { eth: rows[0]?.eth ?? 0, usd: rows[0]?.usd ?? 0 };
  }

  const minutes = timeframeMinutesMap[timeframe];
  const rows = await sql<{ eth: number; usd: number }[]>`
      SELECT
        SUM(contract_creation_sum) AS eth,
        SUM(contract_creation_sum * eth_price / POWER(10, 18)) AS usd
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
  contractRows: LeaderboardRowWithFamDetails[],
  ethTransferBaseFees: BaseFees,
  contractCreationBaseFees: BaseFees,
): LeaderboardEntry[] => {
  const contractEntries: ContractEntry[] = contractRows.map((row) => ({
    fees: Number(row.baseFees.eth),
    feesUsd: Number(row.baseFees.usd),
    id: row.contractAddress,
    name: row.name || row.contractAddress,
    image: row.imageUrl,
    type: "contract",
    address: row.contractAddress,
    isBot: row.isBot,
    category: row.category,
    twitterHandle: row.twitterHandle,
    bio: row.bio,
    followersCount: row.followersCount,
    famFollowerCount: row.famFollowerCount,
    twitterName: row.twitterName,
  }));
  const contractCreationEntry: ContractCreationsEntry = {
    fees: contractCreationBaseFees.eth,
    feesUsd: contractCreationBaseFees.usd,
    id: "contract-creations",
    name: "Contract creations",
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

export const extendRowsWithFamDetails = (
  leaderboardRows: LeaderboardRow[],
): T.Task<LeaderboardRowWithFamDetails[]> =>
  pipe(
    leaderboardRows,
    A.map((row) => row.twitterHandle),
    A.map(O.fromNullable),
    A.compact,
    (list) => new Set(list),
    (set) => Array.from(set),
    FamService.getDetails,
    T.map((famDetails) => {
      const map = new Map<string, FamDetails>();
      famDetails.forEach((famDetail) => {
        map.set(famDetail.handle, famDetail);
      });
      return pipe(
        leaderboardRows,
        A.map((row) => {
          if (row.twitterHandle === null) {
            return {
              ...row,
              baseFees: {
                eth: row.baseFees,
                usd: row.baseFeesUsd,
              },
              bio: null,
              followersCount: null,
              famFollowerCount: null,
              twitterName: null,
            };
          }

          const detail = map.get(row.twitterHandle);
          if (detail === undefined) {
            // Fam service did not have details for this twitter handle.
            return {
              ...row,
              baseFees: {
                eth: row.baseFees,
                usd: row.baseFeesUsd,
              },
              bio: null,
              followersCount: null,
              famFollowerCount: null,
              twitterName: null,
            };
          }

          return {
            ...row,
            baseFees: {
              eth: row.baseFees,
              usd: row.baseFeesUsd,
            },
            bio: detail.bio,
            followersCount: detail.followersCount,
            famFollowerCount: detail.famFollowerCount,
            twitterName: detail.name,
          };
        }),
      );
    }),
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
