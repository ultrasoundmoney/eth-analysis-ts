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
