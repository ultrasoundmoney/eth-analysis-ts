import { sqlT } from "./db.js";
import { WeiBI } from "./eth_units.js";
import { flow, O, OAlt, pipe, T, TAlt } from "./fp.js";
import * as Log from "./log.js";
import * as TimeFrames from "./time_frames.js";
import { TimeFrameNext } from "./time_frames.js";
import {
  londonHardForkBlockNumber,
  mergeBlockNumber,
} from "./blocks/blocks.js";
import { Usd } from "./usd_scaling.js";

export type FeesBurnedT = {
  feesBurned5m: number;
  feesBurned5mUsd: number;
  feesBurned1h: number;
  feesBurned1hUsd: number;
  feesBurned24h: number;
  feesBurned24hUsd: number;
  feesBurned7d: number;
  feesBurned7dUsd: number;
  feesBurned30d: number;
  feesBurned30dUsd: number;
  feesBurnedSinceMerge: number;
  feesBurnedSinceMergeUsd: number;
  feesBurnedSinceBurn: number;
  feesBurnedSinceBurnUsd: number;
};

export type PreciseBaseFeeSum = {
  eth: WeiBI;
  usd: Usd;
};

export const getFeeBurn = (timeFrame: TimeFrameNext) =>
  pipe(
    timeFrame,
    (timeFrame: TimeFrameNext) =>
      timeFrame === "since_merge"
        ? sqlT<{ eth: string | null; usd: number }[]>`
      SELECT
        SUM(gas_used::numeric(78) * base_fee_per_gas::numeric(78)) AS eth,
        SUM(gas_used::float8 * base_fee_per_gas::float8 * eth_price / 1e18) AS usd
      FROM blocks
      WHERE number >= ${mergeBlockNumber}
    `
        : timeFrame == "since_burn"
        ? sqlT<{ eth: string | null; usd: number }[]>`
      SELECT
        SUM(gas_used::numeric(78) * base_fee_per_gas::numeric(78)) AS eth,
        SUM(gas_used::float8 * base_fee_per_gas::float8 * eth_price / 1e18) AS usd
      FROM blocks
      WHERE number >= ${londonHardForkBlockNumber}
    `
        : sqlT<{ eth: string | null; usd: number }[]>`
      SELECT
        SUM(gas_used::numeric(78) * base_fee_per_gas::numeric(78)) AS eth,
        SUM(gas_used::float8 * base_fee_per_gas::float8 * eth_price / 1e18) AS usd
      FROM blocks
      WHERE mined_at >= NOW() - ${TimeFrames.intervalSqlMapNext[timeFrame]}::interval
    `,
    T.map(
      flow(
        (rows) => rows[0],
        O.fromNullable,
        O.map((row) => ({
          eth: pipe(
            row.eth,
            O.fromNullable,
            O.map(BigInt),
            O.getOrElse(() => 0n),
          ),
          usd: row.usd,
        })),
        O.getOrElse(() => {
          Log.warn(
            `tried to get fee burn for timeframe: ${timeFrame}, but interval was empty, returning 0`,
          );
          return {
            eth: 0n,
            usd: 0,
          };
        }),
      ),
    ),
  );

type FeeBurns = Record<TimeFrameNext, PreciseBaseFeeSum>;

export const getFeeBurnsOld = () =>
  pipe(
    TimeFrames.timeFramesNext,
    T.traverseArray(
      (timeFrame) =>
        TAlt.seqTPar(T.of(timeFrame), getFeeBurn(timeFrame)) as T.Task<
          [TimeFrameNext, PreciseBaseFeeSum]
        >,
    ),
    T.map((entries) => Object.fromEntries(entries) as FeeBurns),
    T.map((feeBurns) => ({
      feesBurned5m: Number(feeBurns.m5.eth),
      feesBurned5mUsd: feeBurns.m5.usd,
      feesBurned1h: Number(feeBurns.h1.eth),
      feesBurned1hUsd: feeBurns.h1.usd,
      feesBurned24h: Number(feeBurns.d1.eth),
      feesBurned24hUsd: feeBurns.d1.usd,
      feesBurned7d: Number(feeBurns.d7.eth),
      feesBurned7dUsd: feeBurns.d7.usd,
      feesBurned30d: Number(feeBurns.d30.eth),
      feesBurned30dUsd: feeBurns.d30.usd,
      feesBurnedSinceMerge: Number(feeBurns.since_merge.eth),
      feesBurnedSinceMergeUsd: feeBurns.since_merge.usd,
      feesBurnedSinceBurn: Number(feeBurns.since_burn.eth),
      feesBurnedSinceBurnUsd: feeBurns.since_burn.usd,
    })),
  );

export const getFeeBurns = () =>
  pipe(
    TimeFrames.timeFramesNext,
    T.traverseArray(
      (timeFrame) =>
        TAlt.seqTPar(T.of(timeFrame), getFeeBurn(timeFrame)) as T.Task<
          [TimeFrameNext, PreciseBaseFeeSum]
        >,
    ),
    T.map(
      (entries) =>
        Object.fromEntries(entries) as Record<TimeFrameNext, PreciseBaseFeeSum>,
    ),
  );
