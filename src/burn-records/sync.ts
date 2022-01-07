import * as Blocks from "../blocks/blocks.js";
import { O, pipe, T, TAlt } from "../fp.js";
import * as TimeFrames from "../time_frames.js";
import { TimeFrame } from "../time_frames.js";
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
  timeFrame: TimeFrame,
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

const initTimeFrame = (timeFrame: TimeFrame) =>
  pipe(
    TAlt.seqTParT(BurnRecords.getLastIncludedBlock(), () =>
      Blocks.getLastStoredBlock(),
    ),
    T.chain(([lastIncludedBlock]) =>
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

export const init = () =>
  pipe(
    TimeFrames.timeFrames,
    T.traverseArray(initTimeFrame),
    TAlt.concatAllVoid,
  );
