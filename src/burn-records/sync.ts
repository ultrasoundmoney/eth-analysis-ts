import * as Blocks from "../blocks/blocks.js";
import { O, pipe, T, TAlt } from "../fp.js";
import * as Performance from "../performance.js";
import * as TimeFrames from "../time_frames.js";
import * as BurnRecords from "./burn_records.js";

const getFirstBlockToInclude = (
  timeFrame: TimeFrames.TimeFrameNext,
  lastIncludedBlock: O.Option<number>,
) =>
  pipe(
    Blocks.getEarliestBlockInTimeFrame(timeFrame),
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
) =>
  pipe(
    TAlt.seqTPar(
      BurnRecords.expireRecordsOutsideTimeFrame(timeFrame),
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
    BurnRecords.getLastIncludedBlock(),
    T.chain((lastIncludedBlock) =>
      pipe(
        TimeFrames.timeFramesNext,
        T.traverseArray((timeFrame) =>
          Performance.measureTaskPerf(
            `sync ${timeFrame} burn records`,
            syncTimeFrame(timeFrame, lastIncludedBlock),
          ),
        ),
      ),
    ),
    T.chain(() => BurnRecords.setLastIncludedBlockIsLatest()),
  );
