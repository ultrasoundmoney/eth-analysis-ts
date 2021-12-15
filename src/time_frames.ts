import * as TimeFrames from "./duration.js";

export const limitedTimeFrames = ["5m", "1h", "24h", "7d", "30d"] as const;
export type LimitedTimeFrame = typeof limitedTimeFrames[number];
export const timeFrames = [...limitedTimeFrames, "all"] as const;
export type TimeFrame = typeof timeFrames[number];

export const intervalSqlMap: Record<LimitedTimeFrame, string> = {
  "5m": "5 minutes",
  "1h": "1 hours",
  "24h": "24 hours",
  "7d": "7 days",
  "30d": "30 days",
};

export const limitedTimeFrameMillisMap: Record<LimitedTimeFrame, number> = {
  "5m": TimeFrames.millisFromMinutes(5),
  "1h": TimeFrames.millisFromHours(1),
  "24h": TimeFrames.millisFromDays(1),
  "7d": TimeFrames.millisFromDays(7),
  "30d": TimeFrames.millisFromDays(30),
};

export const timeFrameMillisMap: Record<LimitedTimeFrame, number> = {
  "5m": TimeFrames.millisFromMinutes(5),
  "1h": TimeFrames.millisFromHours(1),
  "24h": TimeFrames.millisFromHours(24),
  "7d": TimeFrames.millisFromDays(7),
  "30d": TimeFrames.millisFromDays(30),
};
