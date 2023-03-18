import * as Blocks from "../blocks/blocks.js";
import { O, pipe, T, TAlt, TOAlt } from "../fp.js";
import * as Performance from "../performance.js";
import * as TimeFrames from "../time_frames.js";
import * as BurnRecords from "./burn_records.js";

const getFirstBlockToInclude = (
  timeFrame: TimeFrames.TimeFrameNext,
  lastIncludedBlock: O.Option<number>,
) =>
  pipe(
    Blocks.getEarliestBlockInTimeFrame(timeFrame),
    TOAlt.expect(
      `expect blocks table to have an earliest block in time frame for ${timeFrame} during record sync`,
    ),
    T.map((earliestBlockInTimeFrame) =>
      TimeFrames.getEarliestBlockToAdd(
        earliestBlockInTimeFrame,
        lastIncludedBlock,
      ),
    ),
  );

const syncTimeFrame = (
  timeFrame: TimeFrames.TimeFrameNext,
  lastIncludedBlock: O.Option<number>,
  lastStoredBlock: Blocks.BlockV1,
) =>
  pipe(
    TAlt.seqTPar(
      BurnRecords.expireRecordsOutsideTimeFrame(lastStoredBlock, timeFrame),
      pipe(
        getFirstBlockToInclude(timeFrame, lastIncludedBlock),
        T.chain((firstBlockToInclude) =>
          BurnRecords.addRecordsFromBlockAndIncluding(
            timeFrame,
            firstBlockToInclude,
          ),
        ),
      ),
    ),
    T.chain(() =>
      BurnRecords.pruneRecordsBeyondRank(timeFrame, BurnRecords.maxRank),
    ),
  );

export const sync = () =>
  pipe(
    T.Do,
    T.apS("lastStoredBlock", Blocks.getLastStoredBlock()),
    T.apS("lastIncludedBlock", BurnRecords.getLastIncludedBlock()),
    T.chain(({ lastStoredBlock, lastIncludedBlock }) =>
      pipe(
        TimeFrames.timeFramesNext,
        T.traverseArray((timeFrame) =>
          pipe(
            syncTimeFrame(timeFrame, lastIncludedBlock, lastStoredBlock),
            Performance.measureTaskPerf(`sync ${timeFrame} burn records`),
          ),
        ),
      ),
    ),
    T.chain(() => BurnRecords.setLastIncludedBlockIsLatest()),
  );
