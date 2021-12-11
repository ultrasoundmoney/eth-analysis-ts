import * as Blocks from "../blocks/blocks.js";
import * as Log from "../log.js";
import { logPerf } from "../performance.js";
import * as TimeFrames from "../time_frames.js";
import {
  addBlockToState,
  getRecordStatesByTimeFrame,
  recordStates,
  rollbackBlock,
} from "./burn_records.js";

export const onNewBlock = async (block: Blocks.BlockDb) => {
  const t0 = performance.now();
  for (const timeFrame of TimeFrames.limitedTimeFrames) {
    const timeFrameRecordStates = getRecordStatesByTimeFrame(
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
    `burn records rollback to and including block: ${rollbackToAndIncluding}`,
  );

  const latestIncludedBlock = recordStates[0].feeBlocks.peekBack();

  if (latestIncludedBlock === undefined) {
    Log.warn("tried to rollback burn records but no block in fee set sum");
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
    const timeFrameRecordStates = getRecordStatesByTimeFrame(
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
