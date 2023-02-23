import { O, pipe } from "./fp.js";

export const fixedDurationTimeFrames = ["5m", "1h", "24h", "7d", "30d"] as const;
export type FixedDurationTimeFrame = typeof fixedDurationTimeFrames[number];
export const timeFrames = [...fixedDurationTimeFrames, "since_merge", "since_burn"] as const;
export type TimeFrame = typeof timeFrames[number];

export const fixedDurationTimeFramesNext = ["m5", "h1", "d1", "d7", "d30", ] as const;
export type FixedDurationTimeFrameNext = typeof fixedDurationTimeFramesNext[number];
export const timeFramesNext = [...fixedDurationTimeFramesNext, "since_merge", "since_burn"] as const;
export type TimeFrameNext = typeof timeFramesNext[number];

export const intervalSqlMap: Record<FixedDurationTimeFrame, string> = {
  "5m": "5 minutes",
  "1h": "1 hours",
  "24h": "24 hours",
  "7d": "7 days",
  "30d": "30 days",
};

export const intervalSqlMapNext: Record<FixedDurationTimeFrameNext, string> = {
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
