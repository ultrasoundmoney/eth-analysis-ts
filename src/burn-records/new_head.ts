import * as Blocks from "../blocks/blocks.js";
import { NEA, pipe, T, TAlt } from "../fp.js";
import * as TimeFrames from "../time_frames.js";
import * as BurnRecords from "./burn_records.js";
import * as Performance from "../performance.js";

export const onNewBlock = (block: Blocks.BlockV1) =>
  pipe(
    TimeFrames.timeFramesNext,
    T.traverseSeqArray((timeFrame) =>
      pipe(
        TAlt.seqTSeq(
          pipe(
            BurnRecords.expireRecordsOutsideTimeFrame(block, timeFrame),
            Performance.measureTaskPerf(
              `expire records outside time frame ${timeFrame}`,
            ),
          ),
          pipe(
            BurnRecords.addRecordsFromBlockAndIncluding(
              timeFrame,
              block.number,
            ),
            Performance.measureTaskPerf("add records from block and including"),
          ),
        ),
        T.chain(() =>
          pipe(
            BurnRecords.pruneRecordsBeyondRank(timeFrame, BurnRecords.maxRank),
            Performance.measureTaskPerf("prune records beyond rank"),
          ),
        ),
      ),
    ),
    T.chain(() =>
      pipe(
        BurnRecords.setLastIncludedBlock(block.number),
        Performance.measureTaskPerf("set last included block"),
      ),
    ),
  );

export const rollbackBlocks = (
  blocksToRollback: NEA.NonEmptyArray<Blocks.BlockV1>,
) =>
  pipe(blocksToRollback, NEA.sort(Blocks.sortDesc), NEA.last, (block) =>
    pipe(
      BurnRecords.expireRecordsFromAndIncluding(block.number),
      T.chain(() => BurnRecords.setLastIncludedBlock(block.number - 1)),
    ),
  );
