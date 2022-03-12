import * as Blocks from "../blocks/blocks.js";
import { NEA, pipe, T, TAlt } from "../fp.js";
import * as TimeFrames from "../time_frames.js";
import * as BurnRecords from "./burn_records.js";

export const onNewBlock = (block: Blocks.BlockDb) =>
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

export const rollbackBlocks = (
  blocksToRollback: NEA.NonEmptyArray<Blocks.BlockDb>,
) =>
  pipe(blocksToRollback, NEA.sort(Blocks.sortDesc), NEA.last, (block) =>
    pipe(
      BurnRecords.expireRecordsFromAndIncluding(block.number),
      T.chain(() => BurnRecords.setLastIncludedBlock(block.number - 1)),
    ),
  );
