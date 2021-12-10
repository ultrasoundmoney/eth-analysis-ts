import _ from "lodash";
import * as Blocks from "../blocks/blocks.js";
import * as Cartesian from "../cartesian.js";
import { denominations } from "../denominations.js";
import * as Log from "../log.js";
import { logPerf } from "../performance.js";
import * as TimeFrames from "../time_frames.js";
import { TimeFrame } from "../time_frames.js";
import {
  addBlockToState,
  getIsBlockWithinReferenceMaxAge,
  granularities,
  Granularity,
  granularityMillisMap,
  makeRecordState,
  RecordState,
  rollbackBlock,
  sortings,
} from "./burn_records.js";

const getIsGranularityEnabledForTimeFrame = (
  granularity: Granularity,
  timeFrame: TimeFrame,
) => {
  if (granularity === "block") {
    return true;
  }

  if (timeFrame === "all") {
    return true;
  }

  const granularityMillis = granularityMillisMap[granularity];
  const timeFrameMillis = TimeFrames.timeFrameMillisMap[timeFrame];

  if (timeFrameMillis > granularityMillis) {
    return true;
  }

  return false;
};

const recordStates = Cartesian.make4(
  denominations,
  granularities,
  TimeFrames.limitedTimeFrames,
  sortings,
)
  .map(([denomination, granularity, timeFrame, sorting]) =>
    getIsGranularityEnabledForTimeFrame(granularity, timeFrame)
      ? makeRecordState(denomination, granularity, sorting, timeFrame)
      : undefined,
  )
  .filter((v): v is RecordState => v !== undefined);

const getRecordStateByTimeFrame = (
  recordStates: RecordState[],
  timeFrame: TimeFrame,
): RecordState[] =>
  recordStates.filter((recordState) => recordState.timeFrame === timeFrame)!;

export const init = async () => {
  Log.debug("init burn records limited time frames");
  const lastStoredBlock = await Blocks.getLastStoredBlock();
  const tGetd30Blocks = performance.now();
  const d30OldBlock = await Blocks.getPastBlock(lastStoredBlock, "30 days");
  const d30Blocks = await Blocks.getFeeBlocks(
    d30OldBlock.number,
    lastStoredBlock.number,
  );
  logPerf(
    "init burn records limited timeframes, reading d30 blocks",
    tGetd30Blocks,
  );

  const tAddBlocks = performance.now();
  for (const timeFrame of TimeFrames.limitedTimeFrames) {
    const getIsBlockWithinTimeFrame = getIsBlockWithinReferenceMaxAge(
      TimeFrames.limitedTimeFrameMillisMap[timeFrame],
      lastStoredBlock,
    );
    const blocksInTimeFrame = _.dropWhile(d30Blocks, getIsBlockWithinTimeFrame);
    const blocksOldToNew = blocksInTimeFrame.reverse();
    const timeFrameRecordStates = getRecordStateByTimeFrame(
      recordStates,
      timeFrame,
    );
    for (const block of blocksOldToNew) {
      const tasks = timeFrameRecordStates.map(
        (recordState) => () => addBlockToState(recordState, block),
      );
      await Promise.all(tasks);
    }
  }
  logPerf(
    "init burn records limited time frames, adding blocks to time frames",
    tAddBlocks,
  );
};

export const onNewBlock = async (block: Blocks.BlockDb) => {
  const t0 = performance.now();
  for (const timeFrame of TimeFrames.limitedTimeFrames) {
    const timeFrameRecordStates = getRecordStateByTimeFrame(
      recordStates,
      timeFrame,
    );
    const tasks = timeFrameRecordStates.map((recordState) =>
      addBlockToState(recordState, block),
    );
    await Promise.all(tasks);
  }
  logPerf("add block to burn record all took: ", t0);
};

export const onRollback = async (
  rollbackToAndIncluding: number,
): Promise<void> => {
  Log.debug(
    `burn record limited time frames rollback to and including block: ${rollbackToAndIncluding}`,
  );

  const latestIncludedBlock = _.last(recordStates[0]["feeBlocks"]);

  if (latestIncludedBlock === undefined) {
    Log.warn(
      "tried to rollback burn-records-limited-time-frame but no block in fee set sum",
    );
    return undefined;
  }

  if (latestIncludedBlock.number < rollbackToAndIncluding) {
    Log.debug(
      `rollback to ${rollbackToAndIncluding}, but burn records at ${latestIncludedBlock.number}`,
    );
    return undefined;
  }

  const blocksToRollback = Blocks.getBlockRange(
    rollbackToAndIncluding,
    latestIncludedBlock.number,
  ).reverse();

  for (const timeFrame of TimeFrames.limitedTimeFrames) {
    const timeFrameRecordStates = getRecordStateByTimeFrame(
      recordStates,
      timeFrame,
    );

    const tasks = timeFrameRecordStates.map(async (recordState) => {
      for (const blockNumber of blocksToRollback) {
        const [block] = await Blocks.getBlocks(blockNumber, blockNumber);
        rollbackBlock(recordState, block);
      }
    });

    await Promise.all(tasks);
  }
};
