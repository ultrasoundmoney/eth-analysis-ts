import * as TimeFrames from "./duration.js";
import { O, pipe } from "./fp.js";

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

export const limitedTimeFrameMillisMap: Record<
  LimitedPlusMergeTimeFrame,
  number
> = {
  "5m": TimeFrames.millisFromMinutes(5),
  "1h": TimeFrames.millisFromHours(1),
  "24h": TimeFrames.millisFromDays(1),
  "7d": TimeFrames.millisFromDays(7),
  "30d": TimeFrames.millisFromDays(30),
  since_merge: TimeFrames.millisFromDays(30),
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
