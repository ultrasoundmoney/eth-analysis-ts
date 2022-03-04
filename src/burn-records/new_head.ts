import { BlockDb } from "../blocks/blocks.js";
import { pipe, T, TAlt } from "../fp.js";
import * as TimeFrames from "../time_frames.js";
import * as BurnRecords from "./burn_records.js";

export const onNewBlock = (block: BlockDb) =>
  pipe(
    TimeFrames.timeFramesNext,
    T.traverseArray((timeFrame) =>
      pipe(
        TAlt.seqTPar(
          BurnRecords.expireRecordsOutsideTimeFrame(timeFrame),
          BurnRecords.addRecordsFromBlockAndIncluding(timeFrame, block.number),
        ),
        T.chain(() =>
          BurnRecords.pruneRecordsBeyondRank(timeFrame, BurnRecords.maxRank),
        ),
      ),
    ),
    T.chain(() => BurnRecords.setLastIncludedBlock(block.number)),
  );

export const onRollback = (rollbackToAndIncluding: number) =>
  pipe(
    BurnRecords.expireRecordsFromAndIncluding(rollbackToAndIncluding),
    T.chain(() => BurnRecords.setLastIncludedBlock(rollbackToAndIncluding - 1)),
  );
