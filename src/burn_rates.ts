import * as DateFns from "date-fns";
import * as Blocks from "./blocks/blocks.js";
import { FeesBurnedT } from "./fee_burn.js";
import { FixedDurationTimeFrame } from "./time_frames.js";

export type BurnRatesT = {
  burnRate5m: number;
  burnRate5mUsd: number;
  burnRate1h: number;
  burnRate1hUsd: number;
  burnRate24h: number;
  burnRate24hUsd: number;
  burnRate7d: number;
  burnRate7dUsd: number;
  burnRate30d: number;
  burnRate30dUsd: number;
  burnRateSinceMerge: number;
  burnRateSinceMergeUsd: number;
  burnRateAll: number;
  burnRateAllUsd: number;
};

const timeframeMinutesMap: Record<FixedDurationTimeFrame, number> = {
  "5m": 5,
  "1h": 60,
  "24h": 24 * 60,
  "7d": 7 * 24 * 60,
  "30d": 30 * 24 * 60,
};

export const calcBurnRates = (feeBurns: FeesBurnedT): BurnRatesT => ({
  burnRate5m: feeBurns.feesBurned5m / timeframeMinutesMap["5m"],
  burnRate5mUsd: feeBurns.feesBurned5mUsd / timeframeMinutesMap["5m"],
  burnRate1h: feeBurns.feesBurned1h / timeframeMinutesMap["1h"],
  burnRate1hUsd: feeBurns.feesBurned1hUsd / timeframeMinutesMap["1h"],
  burnRate24h: feeBurns.feesBurned24h / timeframeMinutesMap["24h"],
  burnRate24hUsd: feeBurns.feesBurned24hUsd / timeframeMinutesMap["24h"],
  burnRate7d: feeBurns.feesBurned7d / timeframeMinutesMap["7d"],
  burnRate7dUsd: feeBurns.feesBurned7dUsd / timeframeMinutesMap["7d"],
  burnRate30d: feeBurns.feesBurned30d / timeframeMinutesMap["30d"],
  burnRate30dUsd: feeBurns.feesBurned30dUsd / timeframeMinutesMap["30d"],
  burnRateSinceMerge:
    feeBurns.feesBurnedSinceMerge /
    DateFns.differenceInMinutes(new Date(), Blocks.mergeBlockDate),
  burnRateSinceMergeUsd:
    feeBurns.feesBurnedSinceMergeUsd /
    DateFns.differenceInMinutes(new Date(), Blocks.mergeBlockDate),
  burnRateAll:
    feeBurns.feesBurnedAll /
    DateFns.differenceInMinutes(new Date(), Blocks.londonHardForkBlockDate),
  burnRateAllUsd:
    feeBurns.feesBurnedAllUsd /
    DateFns.differenceInMinutes(new Date(), Blocks.londonHardForkBlockDate),
});
