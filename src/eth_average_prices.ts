import QuickLRU from "quick-lru";
import { sql } from "./db.js";
import * as Duration from "./duration.js";
import { O, pipe, T, TAlt, TO, TOAlt } from "./fp.js";
import * as Log from "./log.js";
import { LimitedTimeFrameNext, TimeFrameNext } from "./time_frames.js";
import * as TimeFrames from "./time_frames.js";

type AverageEthPrice = {
  m5: number;
  h1: number;
  // @deprecated remove when frontend is switched to d1
  h24: number;
  d1: number;
  d7: number;
  d30: number;
  all: number;
};

type AveragePrice = { ethPriceAverage: number };

const getAllAveragePrice = (): T.Task<number> =>
  pipe(
    () => sql<AveragePrice[]>`
      SELECT AVG(eth_price) AS eth_price_average FROM blocks
      WHERE number >= 12965000
    `,
    T.map((rows) => rows[0]?.ethPriceAverage ?? 0),
  );

const getTimeframeAverage = (timeframe: LimitedTimeFrameNext): T.Task<number> =>
  pipe(
    () => sql<AveragePrice[]>`
        SELECT
          AVG(eth_price) AS eth_price_average
        FROM blocks
        WHERE mined_at >= now() - ${TimeFrames.intervalSqlMapNext[timeframe]}::interval
        AND number >= 12965000
      `,
    T.map((rows) => rows[0]?.ethPriceAverage ?? 0),
  );

const averagePriceCache = new QuickLRU<TimeFrameNext, number>({
  maxSize: 6,
});

const timeFrameMaxAgeMap: Record<TimeFrameNext, number> = {
  m5: Duration.millisFromSeconds(3),
  h1: Duration.millisFromMinutes(2),
  d1: Duration.millisFromMinutes(30),
  d7: Duration.millisFromMinutes(30),
  d30: Duration.millisFromMinutes(30),
  all: Duration.millisFromMinutes(30),
};

const getCachedAveragePrice = (timeFrame: TimeFrameNext) =>
  pipe(
    averagePriceCache.get(timeFrame),
    O.fromNullable,
    TO.fromOption,
    TO.chainFirstIOK(() => () => {
      Log.debug(`get eth average price for time frame: ${timeFrame} cache hit`);
    }),
  );

const getFreshAveragePrice = (timeFrame: TimeFrameNext) =>
  pipe(
    timeFrame === "all" ? getAllAveragePrice() : getTimeframeAverage(timeFrame),
    T.chainFirstIOK((value) => () => {
      Log.debug(
        `get eth average price for time frame: ${timeFrame} cache miss`,
      );
      const maxAge = timeFrameMaxAgeMap[timeFrame];
      averagePriceCache.set(timeFrame, value, { maxAge: maxAge });
    }),
    T.map(O.some),
  );

const getTimeFrameAverage = (timeFrame: TimeFrameNext) =>
  pipe(
    getCachedAveragePrice(timeFrame),
    TO.alt(() => getFreshAveragePrice(timeFrame)),
    TOAlt.getOrThrow(
      "expected getAverage to always return a number but got none",
    ),
  );

export const getAveragePrice = (): T.Task<AverageEthPrice> =>
  TAlt.seqSParT({
    m5: getTimeFrameAverage("m5"),
    h1: getTimeFrameAverage("h1"),
    h24: getTimeFrameAverage("d1"),
    d1: getTimeFrameAverage("d1"),
    d7: getTimeFrameAverage("d7"),
    d30: getTimeFrameAverage("d30"),
    all: getTimeFrameAverage("all"),
  });
