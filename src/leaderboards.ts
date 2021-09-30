import * as A from "fp-ts/lib/Array.js";
import * as Contracts from "./contracts.js";
import * as T from "fp-ts/lib/Task.js";
import { pipe } from "fp-ts/lib/function.js";
import { sql } from "./db.js";

export type Timeframe = LimitedTimeframe | "all";
export type LimitedTimeframe = "5m" | "1h" | "24h" | "7d" | "30d";

export type LeaderboardRow = {
  contractAddress: string;
  name: string;
  isBot: boolean;
  baseFees: number;
  imageUrl: string | undefined;
};

// Name is undefined because we don't always know the name for a contract. Image is undefined because we don't always have an image for a contract. Address is undefined because base fees paid for ETH transfers are shared between many addresses.
export type LeaderboardEntry = {
  name: string | undefined;
  image: string | undefined;
  fees: number;
  id: string;
  type: "eth-transfers" | "bot" | "other" | "contract-creations";
};

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
  contractRows: LeaderboardRow[],
  ethTransferBaseFees: number,
  contractCreationBaseFees: number,
): LeaderboardEntry[] => {
  const contractEntries: LeaderboardEntry[] = contractRows.map(
    ({ contractAddress, baseFees, name, isBot, imageUrl }) => ({
      fees: Number(baseFees),
      id: contractAddress,
      name: name || contractAddress,
      image: imageUrl,
      type: isBot ? "bot" : "other",
    }),
  );
  const contractCreationEntry: LeaderboardEntry = {
    fees: contractCreationBaseFees,
    id: "contract-creations",
    image: undefined,
    name: "Contract creations",
    type: "contract-creations",
  };
  const ethTransfersEntry: LeaderboardEntry = {
    fees: ethTransferBaseFees,
    id: "eth-transfers",
    image: undefined,
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
