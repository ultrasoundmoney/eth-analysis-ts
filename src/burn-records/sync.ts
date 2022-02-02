import * as Blocks from "../blocks/blocks.js";
import { O, pipe, T, TAlt } from "../fp.js";
import * as Performance from "../performance.js";
import * as TimeFrames from "../time_frames.js";
import * as BurnRecords from "./burn_records.js";

const getEarliestBlockToAddAll = (lastIncludedBlock: O.Option<number>) =>
  pipe(
    lastIncludedBlock,
    O.match(
      () => Blocks.londonHardForkBlockNumber,
      (lastIncludedBlock) => lastIncludedBlock + 1,
    ),
  );

const getEarliestBlockToAddLimitedTimeFrames = (
  earliestBlockInTimeFrame: number,
  lastIncludedBlock: O.Option<number>,
) =>
  pipe(
    lastIncludedBlock,
    O.match(
      () => earliestBlockInTimeFrame,
      (lastIncludedBlock) =>
        lastIncludedBlock > earliestBlockInTimeFrame
          ? lastIncludedBlock + 1
          : earliestBlockInTimeFrame,
    ),
  );

const getFirstBlockToInclude = (
  timeFrame: TimeFrames.TimeFrameNext,
  lastIncludedBlock: O.Option<number>,
) =>
  timeFrame === "all"
    ? T.of(getEarliestBlockToAddAll(lastIncludedBlock))
    : pipe(
        Blocks.getEarliestBlockInTimeFrame(timeFrame),
        T.map((earliestBlockInTimeFrame) =>
          getEarliestBlockToAddLimitedTimeFrames(
            earliestBlockInTimeFrame,
            lastIncludedBlock,
          ),
        ),
      );

const syncTimeFrame = (timeFrame: TimeFrames.TimeFrameNext) =>
  pipe(
    BurnRecords.getLastIncludedBlock(),
    T.chain((lastIncludedBlock) =>
      TAlt.seqTParT(
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
    ),
    T.chain(() =>
      BurnRecords.pruneRecordsBeyondRank(timeFrame, BurnRecords.maxRank),
    ),
    T.chain(() => BurnRecords.setLastIncludedBlockIsLatest()),
    T.map(() => undefined),
  );

export const sync = () =>
  pipe(
    TimeFrames.timeFramesNext,
    T.traverseArray((timeFrame) =>
      Performance.measureTaskPerf(
        `sync ${timeFrame} burn records`,
        syncTimeFrame(timeFrame),
      ),
    ),
    TAlt.concatAllVoid,
  );
