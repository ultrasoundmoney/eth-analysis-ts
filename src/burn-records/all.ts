import _ from "lodash";
import PQueue from "p-queue";
import * as Blocks from "../blocks/blocks.js";
import { BlockDb, FeeBlockRow } from "../blocks/blocks.js";
import * as Cartesian from "../cartesian.js";
import * as DateFnsAlt from "../date_fns_alt.js";
import { sql } from "../db.js";
import * as Denominations from "../denominations.js";
import { Denomination, denominations } from "../denominations.js";
import * as Duration from "../duration.js";
import { A, B, O, Ord, OrdM, pipe } from "../fp.js";
import * as Log from "../log.js";
import { getLastAnalyzedBlockNumber } from "./analysis_state.js";
import {
  FeeBlock,
  FeeRecord,
  FeeRecordMap,
  FeeSetMap,
  FeeSetSum,
  granularities,
  Granularity,
  makeFeeSetMap,
  makeRecordMap,
  Sorting,
  sortings,
} from "./burn_records.js";

// pipe(
//   Cartesian.make2(Denominations.denominations, granularities),
//   A.reduce({} as FeeSetMap, (map, [denomination, granularity]) => {
//     map[granularity] = map[granularity] ?? {};
//     map[granularity][denomination] = { sum: 0n, blocks: [] };
//     return map;
//   }),
// );

// The candidate map keeps track of sets of blocks and their corresponding fee sum. It updates in streaming fashion.
export const feeSetMap: FeeSetMap = makeFeeSetMap();

// Tracks fee records.
export const feeRecordMap: FeeRecordMap = makeRecordMap();

export const sumFeeBlocks = (blocks: FeeBlock[]): bigint =>
  pipe(
    blocks,
    A.reduce(0n, (sum, block) => sum + block.fees),
  );

export const getIsBlockWithinReferenceMaxAge =
  (maxAge: number, referenceBlock: FeeBlock) => (targetBlock: FeeBlock) =>
    DateFnsAlt.millisecondsBetweenAbs(
      referenceBlock.minedAt,
      targetBlock.minedAt,
    ) <= maxAge;

const getIsNewRecord = (
  ordering: Ord<FeeRecord>,
  candidate: FeeRecord,
  incumbent: FeeRecord,
) => OrdM.gt(ordering)(candidate, incumbent);

type MergeResult = { isNewRecordSet: boolean; feeRecords: FeeRecord[] };

export const mergeCandidate = (
  ordering: Ord<FeeRecord>,
  feeRecords: FeeRecord[],
  candidateRecord: FeeRecord,
): MergeResult => {
  return pipe(
    feeRecords,
    A.lookup(104),
    O.match(
      // We have less than 105 records, any candidate is a record.
      () => ({
        isNewRecordSet: true,
        feeRecords: pipe(
          feeRecords,
          A.append(candidateRecord),
          A.sort(ordering),
        ),
      }),
      (worstRecord) =>
        pipe(
          // If the candidate is not better than the 100th (worst) record we have no new record to set.
          getIsNewRecord(ordering, candidateRecord, worstRecord),
          B.matchW(
            () => ({
              isNewRecordSet: false,
              feeRecords,
            }),
            () => ({
              isNewRecordSet: true,
              feeRecords: pipe(
                feeRecords,
                A.append(candidateRecord),
                A.sort(ordering),
                A.takeLeft(105),
              ),
            }),
          ),
        ),
    ),
  );
};

export const orderingMap: Record<Sorting, Ord<FeeRecord>> = {
  min: OrdM.fromCompare((first, second) =>
    first.feeSum < second.feeSum ? -1 : first.feeSum === second.feeSum ? 0 : 1,
  ),
  max: OrdM.fromCompare((first, second) =>
    first.feeSum > second.feeSum ? -1 : first.feeSum === second.feeSum ? 0 : 1,
  ),
};

export const expireOldBlocks = (
  maxAge: number,
  referenceDate: Date,
  inScopeBlocks: FeeBlock[],
): FeeBlock[] =>
  pipe(
    inScopeBlocks,
    A.filter(
      (block) =>
        DateFnsAlt.millisecondsBetweenAbs(referenceDate, block.minedAt) <=
        maxAge,
    ),
  );

const granularityMillisMap = {
  block: 0,
  m5: Duration.millisFromMinutes(5),
  h1: Duration.millisFromHours(1),
  d1: Duration.millisFromHours(24),
  d7: Duration.millisFromHours(24 * 7),
};

const feeBlockFromBlock = (
  denomination: Denomination,
  block: FeeBlockRow,
): FeeBlock => {
  const feesWei = block.gasUsed * block.baseFeePerGas;
  return {
    number: block.number,
    minedAt: block.minedAt,
    fees:
      denomination === "eth"
        ? feesWei
        : // TODO: store and retrieve eth price in cents.
          (feesWei * BigInt(Math.round(block.ethPrice * 100))) / 10n ** 18n,
  };
};

const storeLastAnalyzed = async (lastAnalyzedBlock: number): Promise<void> => {
  await sql`
    INSERT INTO analysis_state (
      key,
      last_analyzed_block
    ) VALUES (
      'burn_records_all',
      NULL
    ) ON CONFLICT (key) DO UPDATE SET
      last_analyzed_block = ${lastAnalyzedBlock}
  `;

  return undefined;
};

export const storeNewBlockQueue = new PQueue({
  concurrency: 1,
  autoStart: false,
});

type InsertableFeeRecordRow = {
  denomination: string;
  granularity: string;
  sorting: string;
  first_block: number;
  last_block: number;
  fee_sum: string;
};

const rowFromFeeRecord = (
  denomination: Denomination,
  granularity: Granularity,
  sorting: Sorting,
  feeRecord: FeeRecord,
): InsertableFeeRecordRow => ({
  denomination,
  sorting,
  granularity,
  first_block: feeRecord.firstBlock,
  last_block: feeRecord.lastBlock,
  fee_sum: feeRecord.feeSum.toString(),
});

const storeFeeRecords = async (
  denomination: Denomination,
  granularity: Granularity,
  sorting: Sorting,
  feeRecords: FeeRecord[],
): Promise<void> => {
  feeRecordMap[granularity][sorting][denomination] = feeRecords;

  const rows = feeRecords.map((feeRecord) =>
    rowFromFeeRecord(denomination, granularity, sorting, feeRecord),
  );

  await sql.begin(async (sql) => {
    await sql`
      DELETE FROM fee_records
      WHERE denomination = ${denomination}
      AND sorting = ${sorting}
      AND granularity = ${granularity}
    `;
    await sql<InsertableFeeRecordRow[]>`
      INSERT INTO fee_records
        ${sql(rows)}
    `;
  });
};

const storeFeeSetTotal = (
  denomination: Denomination,
  granularity: Granularity,
  candidateRecords: FeeSetSum,
): void => {
  feeSetMap[granularity][denomination] = candidateRecords;
  return undefined;
};

export const addBlock = async (blockToAdd: FeeBlockRow): Promise<void> => {
  const updateFeesSetsAndRecords = async (
    denomination: Denomination,
    granularity: Granularity,
  ) => {
    const feeBlockToAdd = feeBlockFromBlock(denomination, blockToAdd);

    const getIsBlockWithinMaxAge = getIsBlockWithinReferenceMaxAge(
      granularityMillisMap[granularity],
      feeBlockToAdd,
    );

    const feeSetTotal = feeSetMap[granularity][denomination];

    const { left: blocksToRemove, right: blocksToKeep } = pipe(
      feeSetTotal.blocks,
      // To keep things fast we remember the blocks included for a given denomination and granularity, and their fee sum. Depending on the time that passed since the last block, a number of blocks now fall outside the interval of the block's timestamp minus the duration of the granularity. We subtract the fees from those blocks from the running total and drop them from the included block set. We add the newly received block.
      A.partition(getIsBlockWithinMaxAge),
    );

    const newFeeSetTotal: FeeSetSum = {
      sum: feeSetTotal.sum - sumFeeBlocks(blocksToRemove) + feeBlockToAdd.fees,
      blocks: [...blocksToKeep, feeBlockToAdd],
    };

    storeFeeSetTotal(denomination, granularity, newFeeSetTotal);

    const updateRecordsForSorting = async (sorting: Sorting): Promise<void> => {
      const feeRecords = feeRecordMap[granularity][sorting][denomination];

      const ordering = orderingMap[sorting];

      const candidate: FeeRecord = {
        feeSum: newFeeSetTotal.sum,
        firstBlock: newFeeSetTotal.blocks[0].number,
        lastBlock:
          newFeeSetTotal.blocks[newFeeSetTotal.blocks.length - 1].number,
      };

      const { isNewRecordSet, feeRecords: newFeeRecords } = mergeCandidate(
        ordering,
        feeRecords,
        candidate,
      );

      // As storing fee records is expensive (DB write), and infrequent, we only do so when a new record is set.
      if (isNewRecordSet) {
        await storeFeeRecords(
          denomination,
          granularity,
          sorting,
          newFeeRecords,
        );
      }
    };

    const tasks = sortings.map((sorting) => updateRecordsForSorting(sorting));

    await Promise.all(tasks);
  };

  const tasks = Cartesian.make2(denominations, granularities).map(
    ([denomination, granularity]) =>
      updateFeesSetsAndRecords(denomination, granularity),
  );
  await Promise.all(tasks);
  await storeLastAnalyzed(blockToAdd.number);
};

export const onNewBlock = async (block: BlockDb): Promise<void> =>
  storeNewBlockQueue.add(() => addBlock(block));

const removeForDenominationGranularity = async (
  denomination: Denomination,
  granularity: Granularity,
  blockNumber: number,
) => {
  const feeSetTotal = feeSetMap[granularity][denomination];

  // When the active rollback includes more blocks than the length of the fee set, e.g. rolling back more than one block, with 'block' granularity containing only a single block, the fee set will be empty at this point.
  const blockToRemove = _.last(feeSetTotal.blocks);

  const newFeeSetTotal =
    blockToRemove === undefined
      ? { blocks: [], sum: 0n }
      : {
          blocks: feeSetTotal.blocks.slice(0, -1),
          sum: feeSetTotal.sum - blockToRemove.fees,
        };

  const tasks = sortings.map(async (sorting) => {
    const feeRecords = feeRecordMap[granularity][sorting][denomination];

    const newFeeRecords = pipe(
      feeRecords,
      A.filter((feeRecord) => feeRecord.lastBlock !== blockNumber),
    );

    await storeFeeRecords(denomination, granularity, sorting, newFeeRecords);
  });

  await Promise.all(tasks);

  storeFeeSetTotal(denomination, granularity, newFeeSetTotal);
};

const removeBlock = async (blockNumber: number): Promise<void> => {
  const tasks = Cartesian.make2(denominations, granularities).map(
    ([denomination, granularity]) =>
      removeForDenominationGranularity(denomination, granularity, blockNumber),
  );
  await Promise.all(tasks);
};

export const onRollback = async (
  rollbackToAndIncluding: number,
): Promise<void> => {
  Log.debug(
    `burn record rollback to and including block: ${rollbackToAndIncluding}`,
  );
  const latestIncludedBlock = await getLastAnalyzedBlockNumber();

  if (latestIncludedBlock === undefined) {
    Log.warn("burn record rollback, no anylysis state found, skipping");
    return undefined;
  }

  const blocksToRollback = Blocks.getBlockRange(
    rollbackToAndIncluding,
    latestIncludedBlock,
  ).reverse();

  for (const blockNumber of blocksToRollback) {
    await removeBlock(blockNumber);
  }
};

export const granularitySqlMap: Record<Granularity, string> = {
  block: "0 seconds",
  m5: "5 minutes",
  h1: "1 hours",
  d1: "1 days",
  d7: "7 days",
};
