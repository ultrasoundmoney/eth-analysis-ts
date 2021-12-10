import * as Duration from "./duration.js";

export const limitedTimeFrames = ["5m", "1h", "24h", "7d", "30d"] as const;
export type LimitedTimeFrame = typeof limitedTimeFrames[number];
export type TimeFrame = LimitedTimeFrame | "all";

export const intervalSqlMap: Record<LimitedTimeFrame, string> = {
  "5m": "5 minutes",
  "1h": "1 hours",
  "24h": "24 hours",
  "7d": "7 days",
  "30d": "30 days",
};

export const limitedTimeFrameMillisMap: Record<LimitedTimeFrame, number> = {
  "5m": Duration.millisFromMinutes(5),
  "1h": Duration.millisFromHours(1),
  "24h": Duration.millisFromDays(1),
  "7d": Duration.millisFromDays(7),
  "30d": Duration.millisFromDays(30),
};

export const timeFrameMillisMap: Record<LimitedTimeFrame, number> = {
  "5m": Duration.millisFromMinutes(5),
  "1h": Duration.millisFromHours(1),
  "24h": Duration.millisFromHours(24),
  "7d": Duration.millisFromDays(7),
  "30d": Duration.millisFromDays(30),
};
