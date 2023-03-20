import * as DateFns from "date-fns";
import { O, pipe } from "./fp.js";
import { londonHardForkBlockDate, londonHardForkBlockNumber, mergeBlockDate, mergeBlockNumber } from "./blocks/blocks.js";

export const limitedTimeFrames = ["5m", "1h", "24h", "7d", "30d"] as const;
export type LimitedTimeFrame = typeof limitedTimeFrames[number];
export const limitedPlusMergeTimeFrames = [
  ...limitedTimeFrames,
  "since_merge",
] as const;
export type LimitedPlusMergeTimeFrame =
  typeof limitedPlusMergeTimeFrames[number];
export const timeFrames = [
  ...limitedPlusMergeTimeFrames,
  "since_burn",
] as const;
export type TimeFrame = typeof timeFrames[number];

export const limitedTimeFramesNext = ["m5", "h1", "d1", "d7", "d30"] as const;
export type LimitedTimeFrameNext = typeof limitedTimeFramesNext[number];
export const limitedPlusMergeTimeFramesNext = [
  ...limitedTimeFramesNext,
  "since_merge",
] as const;
export type LimitedPlusMergeTimeFrameNext =
  typeof limitedPlusMergeTimeFramesNext[number];
export const timeFramesNext = [
  ...limitedPlusMergeTimeFramesNext,
  "since_burn",
] as const;
export type TimeFrameNext = typeof timeFramesNext[number];

export const intervalSqlMap: Record<LimitedTimeFrame, string> = {
  "5m": "5 minutes",
  "1h": "1 hours",
  "24h": "24 hours",
  "7d": "7 days",
  "30d": "30 days",
};

export const intervalSqlMapNext: Record<LimitedTimeFrameNext, string> = {
  m5: "5 minutes",
  h1: "1 hours",
  d1: "1 days",
  d7: "7 days",
  d30: "30 days",
};

export const getEarliestBlockToAdd = (
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

export const secondsFromTimeFrame = (timeFrame: TimeFrameNext) => {
  switch (timeFrame) {
    case "m5":
      return 5 * 60;
    case "h1":
      return 60 * 60;
    case "d1":
      return 24 * 60 * 60;
    case "d7":
      return 7 * 24 * 60 * 60;
    case "d30":
      return 30 * 24 * 60 * 60;
    case "since_merge":
      return DateFns.differenceInSeconds(new Date(), mergeBlockDate);
    case "since_burn":
      return DateFns.differenceInSeconds(new Date(), londonHardForkBlockDate);
  }
};
