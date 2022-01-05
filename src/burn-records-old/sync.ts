import _ from "lodash";
import makeEta from "simple-eta";
import * as Blocks from "../blocks/blocks.js";
import { BlockDb } from "../blocks/blocks.js";
import * as Log from "../log.js";
import { logPerf } from "../performance.js";
import * as TimeFrames from "../time_frames.js";
import {
  addBlockToState,
  getIsBlockWithinReferenceMaxAge,
  getRecordStatesByTimeFrame,
  recordStates,
} from "./burn_records.js";

export const init = async (lastStoredBlock: BlockDb) => {
  Log.debug("init burn records limited time frames");
  const tGetAllBlocks = performance.now();
  const allBlocks = await Blocks.getFeeBlocks(
    Blocks.londonHardForkBlockNumber,
    lastStoredBlock.number,
  );
  logPerf("init burn records, reading all blocks", tGetAllBlocks);

  const tInitAllState = performance.now();
  for (const timeFrame of TimeFrames.timeFrames) {
    const tFilterBlocks = performance.now();
    const getIsBlockWithinTimeFrame =
      timeFrame === "all"
        ? () => true
        : getIsBlockWithinReferenceMaxAge(
            TimeFrames.limitedTimeFrameMillisMap[timeFrame],
            lastStoredBlock,
          );

    const blocksInTimeFrame = _.dropWhile(allBlocks, getIsBlockWithinTimeFrame);
    const blocksOldToNew = blocksInTimeFrame.reverse();
    logPerf(
      `init burn records, filter time frame ${timeFrame}, ${blocksInTimeFrame.length} blocks`,
      tFilterBlocks,
    );
    const timeFrameRecordStates = getRecordStatesByTimeFrame(
      recordStates,
      timeFrame,
    );

    for (const recordState of timeFrameRecordStates) {
      const eta = makeEta({ max: blocksInTimeFrame.length });
      let blocksDone = 0;
      const logEta = _.throttle((block) => {
        Log.debug(
          `burn records init, time frame: ${timeFrame}, granularity: ${
            recordState.granularity
          }, eta: ${eta.estimate()}s, last block: ${block.number}`,
        );
      }, 2000);

      for (const block of blocksOldToNew) {
        addBlockToState(recordState, block);
        blocksDone = blocksDone + 1;
        eta.report(blocksDone);
        logEta(block);
      }
    }
  }
  logPerf("init burn records, adding blocks to time frames", tInitAllState);
};
