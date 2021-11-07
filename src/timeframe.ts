export const limitedTimeframes = ["5m", "1h", "24h", "7d", "30d"] as const;
export type LimitedTimeframe = typeof limitedTimeframes[number];
export type Timeframe = LimitedTimeframe | "all";

export const intervalSqlMap: Record<LimitedTimeframe, string> = {
  "5m": "5 minutes",
  "1h": "1 hours",
  "24h": "24 hours",
  "7d": "7 days",
  "30d": "30 days",
};
