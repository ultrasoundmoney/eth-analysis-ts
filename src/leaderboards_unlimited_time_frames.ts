import { pipe } from "fp-ts/lib/function.js";
import * as Performance from "./performance.js";
import * as Blocks from "./blocks/blocks.js";
import { sql, sqlT } from "./db.js";
import { A, NEA, O, T } from "./fp.js";
import * as Leaderboards from "./leaderboards.js";
import {
  ContractBaseFeeSums,
  ContractSums,
  LeaderboardRow,
} from "./leaderboards.js";
import * as Log from "./log.js";

type TimeFrame = "all" | "since_merge";

const timeFrameToTableName: Record<TimeFrame, string> = {
  all: "contract_base_fee_sums",
  since_merge: "contract_base_fee_sums_since_merge",
};

export const getNewestIncludedBlockNumber = async (
  timeFrame: TimeFrame,
) => {
  const rows = await sql<{ newestIncludedBlock: number }[]>`
    SELECT newest_included_block
    FROM base_fee_sum_included_blocks
    WHERE timeframe = ${timeFrame}
  `;

  return rows[0]?.newestIncludedBlock ?? undefined;
};

export const setNewestIncludedBlockNumber = async (
  blockNumber: number,
  timeFrame: TimeFrame,
): Promise<void> => {
  await sql`
      INSERT INTO base_fee_sum_included_blocks (
        oldest_included_block,
        newest_included_block,
        timeframe
      )
      VALUES (
        ${Blocks.londonHardForkBlockNumber},
        ${blockNumber},
        ${timeFrame}
      )
      ON CONFLICT (timeframe) DO UPDATE SET
        oldest_included_block = ${Blocks.londonHardForkBlockNumber},
        newest_included_block = ${blockNumber}
    `;
};

const addContractBaseFeeSums = async (
  contractSums: ContractBaseFeeSums,
  timeFrame: TimeFrame,
): Promise<void> => {
  const tableName = timeFrameToTableName[timeFrame];
  if (contractSums.eth.size === 0) {
    return;
  }

  const addressesChunks = pipe(
    Array.from(contractSums.eth.keys()),
    A.chunksOf(10000),
  );

  const promises = addressesChunks.map(async (addresses) => {
    const fees = addresses.map(
      (address) => contractSums.eth.get(address) ?? null,
    );
    const feesUsd = addresses.map(
      (address) => contractSums.usd.get(address) ?? null,
    );
    await sql`
      INSERT INTO ${sql(tableName)} (
        contract_address,
        base_fee_sum,
        base_fee_sum_usd
      )
      SELECT
        UNNEST(${sql.array(addresses)}::text[]),
        UNNEST(${sql.array(fees)}::double precision[]),
        UNNEST(${sql.array(feesUsd)}::double precision[])
      ON CONFLICT (contract_address) DO UPDATE SET
        base_fee_sum =
          ${sql(tableName)}.base_fee_sum + excluded.base_fee_sum::float8,
        base_fee_sum_usd =
          ${sql(tableName)}.base_fee_sum_usd + excluded.base_fee_sum_usd::float8
    `;
  });
  await Promise.all(promises);
};

export type Currency = "eth" | "usd";

const removeContractBaseFeeSums = async (
  contractSums: ContractBaseFeeSums,
  timeFrame: TimeFrame,
): Promise<void> => {
  const tableName = timeFrameToTableName[timeFrame];
  if (contractSums.eth.size === 0) {
    return undefined;
  }
  const addressesChunks = pipe(
    Array.from(contractSums.eth.keys()),
    A.chunksOf(10000),
  );

  const promises = addressesChunks.map(async (addresses) => {
    const fees = addresses.map(
      (address) => contractSums.eth.get(address) ?? null,
    );
    const feesUsd = addresses.map(
      (address) => contractSums.usd.get(address) ?? null,
    );

    await sql`
      UPDATE ${sql(tableName)} SET
        base_fee_sum = ${sql(tableName)}.base_fee_sum - data_table.base_fee_sum,
        base_fee_sum_usd = ${sql(
          tableName,
        )}.base_fee_sum_usd - data_table.base_fee_sum_usd
      FROM (
        SELECT
          UNNEST(${sql.array(addresses)}::text[]) as contract_address,
          UNNEST(${sql.array(fees)}::double precision[]) as base_fee_sum,
          UNNEST(${sql.array(feesUsd)}::double precision[]) as base_fee_sum_usd
      ) as data_table
      WHERE ${sql(tableName)}.contract_address = data_table.contract_address
    `;
  });
  await Promise.all(promises);
};

export const addBlock = async (
  blockNumber: number,
  baseFeeSumsEth: ContractSums,
  baseFeeSumsUsd: ContractSums,
  timeFrame: TimeFrame,
): Promise<void> => {
  await addContractBaseFeeSums(
    { eth: baseFeeSumsEth, usd: baseFeeSumsUsd },
    timeFrame,
  );
  await setNewestIncludedBlockNumber(blockNumber, timeFrame);
};

export const addMissingBlocks = async (
  timeFrame: TimeFrame,
): Promise<void> => {
  const [newestIncludedBlock, lastStoredBlock] = await Promise.all([
    getNewestIncludedBlockNumber(timeFrame),
    Blocks.getLastStoredBlock()(),
  ]);

  if (
    newestIncludedBlock !== undefined &&
    lastStoredBlock.number === newestIncludedBlock
  ) {
    // All blocks already stored. Nothing to do.
    Log.debug("leaderboard all already up to date with latest stored block");
    return undefined;
  }

  const nextBlockToInclude =
    typeof newestIncludedBlock === "number"
      ? newestIncludedBlock + 1
      : timeFrame === "all"
      ? Blocks.londonHardForkBlockNumber
      : Blocks.mergeBlockNumber;

  Log.debug(
    `sync leaderboard ${timeFrame}, ${
      lastStoredBlock.number - nextBlockToInclude
    } blocks to analyze`,
  );

  const rangeBaseFees = await Leaderboards.getRangeBaseFees(
    nextBlockToInclude,
    lastStoredBlock.number,
  )();
  await addContractBaseFeeSums(rangeBaseFees, timeFrame);
  await setNewestIncludedBlockNumber(
    lastStoredBlock.number,
    timeFrame,
  );
};

const getTopBaseFeeContracts = (timeFrame: TimeFrame) =>
  pipe(
    sqlT<LeaderboardRow[]>`
      WITH top_base_fee_contracts AS (
        SELECT
          contract_address,
          base_fee_sum,
          base_fee_sum_usd
        FROM ${sql(timeFrameToTableName[timeFrame])}
        ORDER BY base_fee_sum DESC
        LIMIT 100
      )
      SELECT
        base_fee_sum AS base_fees,
        base_fee_sum_usd AS base_fees_usd,
        category,
        contract_address,
        image_url,
        is_bot,
        name,
        twitter_description AS twitter_bio,
        twitter_handle,
        twitter_name
      FROM top_base_fee_contracts
      JOIN contracts ON address = contract_address
    `,
    T.map(
      A.map((row) => ({
        ...row,
        detail: pipe(
          row.name,
          O.fromNullable,
          O.map((name) => name.split(":")[1]),
          O.map(O.fromNullable),
          O.flatten,
          O.map((detail) => detail.trimStart()),
          O.toNullable,
        ),
      })),
    ),
  );

export const calcLeaderboard = (timeFrame: TimeFrame) => {
  const timeFrameNewWording = timeFrame === "all" ? "since_burn" : timeFrame;
  return pipe(
    T.Do,
    T.bind("topBaseFeeContracts", () =>
      pipe(
        getTopBaseFeeContracts(timeFrame),
        Performance.measureTaskPerf(
          "    get ranked contracts for leaderboard all",
        ),
        T.chain((rows) => Leaderboards.extendRowsWithTwitterDetails(rows)),
        Performance.measureTaskPerf("    add twitter details leaderboard all"),
      ),
    ),
    T.bind("ethTransferBaseFees", () =>
      pipe(
        () => Leaderboards.getEthTransferFeesForTimeframe(timeFrameNewWording),
        Performance.measureTaskPerf(
          "    add eth transfer fees leaderboard all",
        ),
      ),
    ),
    T.bind("contractCreationBaseFees", () =>
      pipe(
        () =>
          Leaderboards.getContractCreationBaseFeesForTimeframe(timeFrameNewWording),
        Performance.measureTaskPerf(
          "    add contract creation fees leaderboard all",
        ),
      ),
    ),
    T.bind("blobBaseFees", () =>
      pipe(
        () =>
          Leaderboards.getBlobBaseFeesForTimeframe(timeFrameNewWording),
        Performance.measureTaskPerf(
          "    add blob fees leaderboard all",
        ),
      ),
    ),
    T.map(
      ({
        topBaseFeeContracts,
        ethTransferBaseFees,
        contractCreationBaseFees,
        blobBaseFees,
      }) =>
        Leaderboards.buildLeaderboard(
          topBaseFeeContracts,
          ethTransferBaseFees,
          contractCreationBaseFees,
          blobBaseFees
        ),
    ),
  );
}

export const rollbackBlocks = (blocks: NEA.NonEmptyArray<Blocks.BlockV1>, timeFrame: TimeFrame) =>
  pipe(
    blocks,
    NEA.sort(Blocks.sortDesc),
    (blocksNewestFirst) =>
      Leaderboards.getRangeBaseFees(
        NEA.last(blocksNewestFirst).number,
        NEA.head(blocksNewestFirst).number,
      ),
    T.chain(
      (sumsToRollback) => () => removeContractBaseFeeSums(sumsToRollback, timeFrame),
    ),
    T.chain(() =>
      pipe(
        blocks,
        NEA.sort(Blocks.sortAsc),
        NEA.head,
        (block) => () => setNewestIncludedBlockNumber(block.number - 1, timeFrame),
      ),
    ),
  );
