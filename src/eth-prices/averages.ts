import QuickLRU from "quick-lru";
import * as Blocks from "../blocks/blocks.js";
import { sql, sqlT, sqlTNotify, sqlTVoid } from "../db.js";
import * as Duration from "../duration.js";
import { flow, O, pipe, T, TAlt, TOAlt } from "../fp.js";
import * as Log from "../log.js";
import { intervalSqlMapNext, TimeFrameNext } from "../time_frames.js";

export type AverageEthPrices = {
  m5: number;
  h1: number;
  d1: number;
  d7: number;
  d30: number;
  since_burn: number;
  all: number;
};

export const averagePricesCacheKey = "average-prices-cache-key";

const averagePriceCache = new QuickLRU<TimeFrameNext, number>({
  maxSize: 6,
});

const timeFrameMaxAgeMap: Record<TimeFrameNext, number> = {
  m5: Duration.millisFromSeconds(3),
  h1: Duration.millisFromMinutes(2),
  d1: Duration.millisFromMinutes(30),
  d7: Duration.millisFromMinutes(30),
  d30: Duration.millisFromMinutes(30),
  since_merge: Duration.millisFromMinutes(30),
  since_burn: Duration.millisFromMinutes(30),
};

const getAveragePriceCache = (timeFrame: TimeFrameNext) =>
  pipe(averagePriceCache.get(timeFrame), O.fromNullable);

const getAveragePriceDb = (timeFrame: TimeFrameNext) =>
  pipe(
    timeFrame,
    (timeFrame) => {
      let blockQuery;
      if (timeFrame == "since_merge" || timeFrame == "since_burn") {
        blockQuery = `number >= ${
          timeFrame == "since_merge"
            ? Blocks.mergeBlockNumber
            : Blocks.londonHardForkBlockNumber
        }`;
      } else {
        blockQuery = `mined_at >= NOW() - ${intervalSqlMapNext[timeFrame]}::interval`;
      }
      return sqlT<{ average: number }[]>`
        SELECT AVG(eth_price) AS average FROM blocks
        WHERE ${blockQuery}
      `;
    },
    T.map(flow((rows) => rows[0]?.average, O.fromNullable)),
    TOAlt.expect(
      "tried to calculate average eth price with zero blocks in target time frame",
    ),
  );

const getTimeFrameAverage = (timeFrame: TimeFrameNext) =>
  pipe(
    getAveragePriceCache(timeFrame),
    O.map(T.of),
    O.getOrElse(() =>
      pipe(
        getAveragePriceDb(timeFrame),
        T.chainFirstIOK((averagePrice) => () => {
          Log.debug(`get ${timeFrame} average eth price, cache miss`);
          const maxAge = timeFrameMaxAgeMap[timeFrame];
          averagePriceCache.set(timeFrame, averagePrice, { maxAge: maxAge });
        }),
      ),
    ),
  );

const updateCache = (averagePrices: AverageEthPrices) => sqlTVoid`
  INSERT INTO key_value_store
    ${sql({
      key: averagePricesCacheKey,
      value: JSON.stringify(averagePrices),
    })}
  ON CONFLICT (key) DO UPDATE SET
    value = excluded.value
`;

export const updateAveragePrices = () =>
  pipe(
    TAlt.seqSPar({
      m5: getTimeFrameAverage("m5"),
      h1: getTimeFrameAverage("h1"),
      d1: getTimeFrameAverage("d1"),
      d7: getTimeFrameAverage("d7"),
      d30: getTimeFrameAverage("d30"),
      since_burn: getTimeFrameAverage("since_burn"),
    }),
    T.map((averagePrices) => ({
      ...averagePrices,
      all: averagePrices.since_burn,
    })),
    T.chain(updateCache),
    T.chainFirst(() => sqlTNotify("cache-update", averagePricesCacheKey)),
  );

export const getAveragePricesCache = () =>
  pipe(
    sqlT<{ value: AverageEthPrices }[]>`
      SELECT value FROM key_value_store
      WHERE key = ${averagePricesCacheKey}
    `,
    T.map((rows) => rows[0]?.value),
  );
