import * as A from "fp-ts/lib/Array.js";
import * as Contracts from "./contracts.js";
import * as FamService from "./fam_service.js";
import * as T from "fp-ts/lib/Task.js";
import { O } from "./fp.js";
import { pipe } from "fp-ts/lib/function.js";
import { sql } from "./db.js";
import { FamDetails } from "./fam_service.js";

export type Timeframe = LimitedTimeframe | "all";
export type LimitedTimeframe = "5m" | "1h" | "24h" | "7d" | "30d";

export type LeaderboardRow = {
  contractAddress: string;
  name: string;
  isBot: boolean;
  baseFees: number;
  imageUrl: string | null;
  twitterHandle: string | null;
  category: string | null;
};

export type LeaderboardRowWithFamDetails = {
  contractAddress: string;
  name: string;
  isBot: boolean;
  baseFees: number;
  imageUrl: string | null;
  twitterHandle: string | null;
  bio: string | null;
  followersCount: number | null;
  famFollowerCount: number | null;
  category: string | null;
};

type ContractEntry = {
  type: "contract";
  name: string | null;
  image: string | null;
  fees: number;
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
  /* deprecated */
  id: string;
};

type ContractCreationsEntry = {
  type: "contract-creations";
  name: string;
  fees: number;
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
};

export const collectInMap = (rows: ContractBaseFeesRow[]): ContractBaseFees =>
  pipe(
    rows,

    A.map((row) => [row.contractAddress, row.baseFees] as [string, number]),
    (entries) => new Map(entries),
  );

export const getRangeBaseFees = (
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
): Promise<number> => {
  if (timeframe === "all") {
    const rows = await sql<{ sum: number }[]>`
      SELECT SUM(eth_transfer_sum) FROM blocks
    `;
    return rows[0]?.sum ?? 0;
  }

  const minutes = timeframeMinutesMap[timeframe];
  const rows_1 = await sql<{ sum: number }[]>`
      SELECT SUM(eth_transfer_sum) FROM blocks
      WHERE mined_at >= NOW() - interval '${sql(String(minutes))} minutes'
  `;
  return rows_1[0]?.sum ?? 0;
};

export const getContractCreationBaseFeesForTimeframe = async (
  timeframe: Timeframe,
): Promise<number> => {
  if (timeframe === "all") {
    const rows = await sql<{ sum: number }[]>`
      SELECT SUM(contract_creation_sum) FROM blocks
    `;
    return rows[0]?.sum ?? 0;
  }

  const minutes = timeframeMinutesMap[timeframe];
  const rows_1 = await sql<{ sum: number }[]>`
      SELECT SUM(contract_creation_sum) FROM blocks
      WHERE mined_at >= NOW() - interval '${sql(String(minutes))} minutes'
  `;
  return rows_1[0]?.sum ?? 0;
};

export const buildLeaderboard = (
  contractRows: LeaderboardRowWithFamDetails[],
  ethTransferBaseFees: number,
  contractCreationBaseFees: number,
): LeaderboardEntry[] => {
  const contractEntries: ContractEntry[] = contractRows.map((row) => ({
    fees: Number(row.baseFees),
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
  }));
  const contractCreationEntry: ContractCreationsEntry = {
    fees: contractCreationBaseFees,
    id: "contract-creations",
    name: "Contract creations",
    type: "contract-creations",
  };
  const ethTransfersEntry: EthTransfersEntry = {
    fees: ethTransferBaseFees,
    id: "eth-transfers",
    name: "ETH transfers",
    type: "eth-transfers",
  };

  // We don't wait and expect the fn to work fast enough.
  Contracts.addContractsMetadata(contractEntries.map((entry) => entry.id));

  return pipe(
    [...contractEntries, ethTransfersEntry, contractCreationEntry],
    A.sort<LeaderboardEntry>({
      compare: (first, second) =>
        first.fees === second.fees ? 0 : first.fees > second.fees ? -1 : 1,
      equals: (first, second) => first.fees === second.fees,
    }),
    A.takeLeft(32),
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
              bio: null,
              followersCount: null,
              famFollowerCount: null,
            };
          }

          const detail = map.get(row.twitterHandle);
          if (detail === undefined) {
            // Fam service did not have details for this twitter handle.
            return {
              ...row,
              bio: null,
              followersCount: null,
              famFollowerCount: null,
            };
          }

          return {
            ...row,
            bio: detail.bio,
            followersCount: detail.followersCount,
            famFollowerCount: detail.famFollowerCount,
          };
        }),
      );
    }),
  );
