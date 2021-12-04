import {
  LimitedTimeFrame,
  limitedTimeFrameMillisMap,
  limitedTimeFrames,
} from "../time_frame.js";
import {
  addBlock,
  FeeRecordMap,
  FeeSetMap,
  getIsBlockWithinReferenceMaxAge,
  makeFeeSetMap,
  makeRecordMap,
  rollbackLastBlock,
} from "./burn_records.js";
import * as Blocks from "../blocks/blocks.js";
import _ from "lodash";
import * as Log from "../log.js";

type FeeSetMapPerTimeFrame = Record<LimitedTimeFrame, FeeSetMap>;

// The candidate map keeps track of sets of blocks and their corresponding fee sum. It updates in streaming fashion.
export const feeSetMapPerTimeFrame: FeeSetMapPerTimeFrame =
  limitedTimeFrames.reduce((map, timeFrame) => {
    map[timeFrame] = makeFeeSetMap();
    return map;
  }, {} as FeeSetMapPerTimeFrame);

type FeeRecordMapPerTimeFrame = Record<LimitedTimeFrame, FeeRecordMap>;

// Tracks fee records.
export const feeRecordMapPerTimeFrame: FeeRecordMapPerTimeFrame =
  limitedTimeFrames.reduce((map, timeFrame) => {
    map[timeFrame] = makeRecordMap();
    return map;
  }, {} as FeeRecordMapPerTimeFrame);

export const init = async () => {
  const lastStoredBlock = await Blocks.getLastStoredBlock();
  const d30OldBlock = await Blocks.getPastBlock(lastStoredBlock, "30 days");
  const d30Blocks = await Blocks.getFeeBlocks(
    d30OldBlock.number,
    lastStoredBlock.number,
  );

  for (const timeFrame of limitedTimeFrames) {
    const getIsBlockWithinTimeFrame = getIsBlockWithinReferenceMaxAge(
      limitedTimeFrameMillisMap[timeFrame],
      lastStoredBlock,
    );
    const blocksInTimeFrame = _.dropWhile(d30Blocks, getIsBlockWithinTimeFrame);
    const blocksOldToNew = blocksInTimeFrame.reverse();
    const feeSetMap = feeSetMapPerTimeFrame[timeFrame];
    const feeRecordMap = feeRecordMapPerTimeFrame[timeFrame];
    for (const block of blocksOldToNew) {
      await addBlock(() => Promise.resolve(), feeSetMap, feeRecordMap, block);
    }
  }
};

export const onNewBlock = async (block: Blocks.BlockDb) => {
  for (const timeFrame of limitedTimeFrames) {
    const feeSetMap = feeSetMapPerTimeFrame[timeFrame];
    const feeRecordMap = feeRecordMapPerTimeFrame[timeFrame];
    await addBlock(() => Promise.resolve(), feeSetMap, feeRecordMap, block);
  }
};

export const onRollback = async (rollbackToAndIncluding: number) => {
  Log.debug(
    `burn record limited time frames rollback to and including block: ${rollbackToAndIncluding}`,
  );

  const latestIncludedBlock = _.last(
    feeSetMapPerTimeFrame["5m"]["block"]["eth"]["blocks"],
  );

  if (latestIncludedBlock === undefined) {
    throw new Error(
      "tried to rollback burn-records-all but no block in fee set sum",
    );
  }

  const blocksToRollback = Blocks.getBlockRange(
    rollbackToAndIncluding,
    latestIncludedBlock.number,
  ).reverse();

  for (const timeFrame of limitedTimeFrames) {
    const feeSetMap = feeSetMapPerTimeFrame[timeFrame];
    const feeRecordMap = feeRecordMapPerTimeFrame[timeFrame];

    for (const blockNumber of blocksToRollback) {
      const [block] = await Blocks.getBlocks(blockNumber, blockNumber);
      await rollbackLastBlock(
        () => Promise.resolve(),
        feeSetMap,
        feeRecordMap,
        block,
      );
    }
  }
};
