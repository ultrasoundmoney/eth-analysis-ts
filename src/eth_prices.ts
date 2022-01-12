import * as DateFns from "date-fns";
import QuickLRU from "quick-lru";
import * as DateFnsAlt from "./date_fns_alt.js";
import { JsTimestamp } from "./date_fns_alt.js";
import { sql, sqlT, sqlTVoid } from "./db.js";
import * as Duration from "./duration.js";
import * as EthPricesFtx from "./eth_prices_ftx.js";
import { E, flow, O, pipe, T, TAlt, TE, TO, TOAlt } from "./fp.js";
import * as Log from "./log.js";
import {
  intervalSqlMapNext,
  LimitedTimeFrameNext,
  TimeFrameNext,
} from "./time_frames.js";

// TODO: move eth_prices... into a folder

export type EthPrice = {
  timestamp: Date;
  ethusd: number;
};

export type BlockForPrice = {
  timestamp: number;
  number: number;
};

/**
 * JS Date rounded to a past minute.
 */
type MinuteDate = Date;

/**
 * ETH price in usd
 */
export type EthUsd = number;

const priceByMinute = new QuickLRU<JsTimestamp, EthUsd>({ maxSize: 4096 });

const getCachedPrice = (dt: Date) =>
  pipe(
    dt,
    DateFns.startOfMinute,
    (dt) => priceByMinute.get(DateFns.getTime(dt)),
    O.fromNullable,
    O.map((ethusd) => ({
      timestamp: DateFns.startOfMinute(dt),
      ethusd,
    })),
  );

const getFreshPrice = (dt: Date) =>
  pipe(
    EthPricesFtx.getPriceByDate(dt),
    TE.chainFirstIOK((historicPrice) => () => {
      priceByMinute.set(
        DateFns.getTime(historicPrice.timestamp),
        historicPrice.ethusd,
      );
    }),
  );

// Execute these sequentially for maximum cache hits.
export const getPriceByDate = (dt: Date) =>
  pipe(
    getCachedPrice(dt),
    TE.fromOption(() => new Error("price not in cache")),
    TE.alt(() => getDbEthPrice(dt)),
    TE.alt(() => getFreshPrice(dt)),
  );

type PriceInsertable = {
  timestamp: MinuteDate;
  ethusd: number;
};

const insertableFromPrice = (ethPrice: EthPrice): PriceInsertable => ({
  timestamp: DateFns.roundToNearestMinutes(ethPrice.timestamp),
  ethusd: ethPrice.ethusd,
});

const storePrice = (ethPrice: EthPrice) =>
  sqlTVoid`
    INSERT INTO eth_prices
      ${sql(insertableFromPrice(ethPrice))}
    ON CONFLICT (timestamp) DO UPDATE SET
      ethusd = excluded.ethusd
  `;

export const storeCurrentEthPrice = () =>
  pipe(
    EthPricesFtx.getPriceByDate(DateFns.startOfMinute(new Date())),
    TE.chainFirstIOK((price) => () => {
      Log.debug(`stored price: ${price.ethusd}, date: ${price.timestamp}`);
    }),
    TE.chain((price) => TE.fromTask(storePrice(price))),
  );

export type HistoricPrice = [JsTimestamp, number];

const getDbEthPrice = (
  timestamp: Date,
): TE.TaskEither<MissingPriceError, EthPrice> =>
  pipe(
    sqlT<{ timestamp: Date; ethusd: number }[]>`
      SELECT
        timestamp,
        ethusd
      FROM eth_prices
      ORDER BY ABS(EXTRACT(epoch FROM (timestamp - ${timestamp}::timestamp )))
      LIMIT 1
    `,
    T.map((rows) => rows[0]),
    T.map(O.fromNullable),
    TE.fromTaskOption(() => new MissingPriceError("eth price table empty")),
  );

class PriceTooOldError extends Error {}

export const getEthPrice = (
  dt: Date,
  maxAgeMillis: number | undefined = undefined,
): TE.TaskEither<Error, EthPrice> => {
  const start = DateFns.startOfMinute(dt);

  return pipe(
    getDbEthPrice(start),
    TE.chainEitherK((ethPrice) => {
      const priceAge = DateFnsAlt.millisecondsBetweenAbs(
        new Date(),
        ethPrice.timestamp,
      );

      if (maxAgeMillis === undefined) {
        return E.right(ethPrice);
      }

      if (priceAge > maxAgeMillis) {
        return E.left(
          new PriceTooOldError(
            `timestamp: ${dt.toISOString()} and closest eth price are more than ${maxAgeMillis} millis apart`,
          ),
        );
      }

      return E.right(ethPrice);
    }),
    TE.mapLeft((e) => {
      if (e instanceof PriceTooOldError) {
        Log.error(
          "DB did not have a fresh enough eth price, falling back to FTX",
        );
      }

      return e;
    }),
    TE.alt(() => getPriceByDate(dt)),
  );
};

export class MissingPriceError extends Error {}
export type Get24hAgoPriceError = MissingPriceError;

export const get24hAgoPrice = () =>
  pipe(
    sqlT<{ ethusd: number }[]>`
        WITH with_diff AS (
          SELECT
          timestamp,
          ethusd,
          ABS(
            EXTRACT(
              epoch FROM (
                timestamp - (NOW() - '1 days'::INTERVAL)
              )
            )
          ) AS time_diff
          FROM eth_prices
        )
        SELECT ethusd FROM with_diff
        WHERE time_diff < 3600
        ORDER BY time_diff
        LIMIT 1
      `,
    T.chain(
      flow(
        (rows) => rows[0]?.ethusd,
        O.fromNullable,
        TE.fromOption(
          () =>
            new MissingPriceError(
              "no price within 1h range on either side of now - 1 day ago",
            ),
        ),
      ),
    ),
  );

const get24hChange = (currentPrice: EthPrice) =>
  pipe(
    get24hAgoPrice(),
    TE.map(
      (price24hAgo) =>
        ((currentPrice.ethusd - price24hAgo) / price24hAgo) * 100,
    ),
  );

type EthStats = {
  usd: number;
  usd24hChange: number;
};

const ethStatsCache = new QuickLRU<string, EthStats>({
  maxSize: 1,
  maxAge: Duration.millisFromSeconds(16),
});

const getCachedEthStats = pipe(ethStatsCache.get("eth-stats"), O.fromNullable);

const makeEthStats = () =>
  pipe(
    TE.Do,
    TE.bind("currentEthPrice", () =>
      getEthPrice(new Date(), Duration.millisFromHours(1)),
    ),
    TE.bind("price24Change", ({ currentEthPrice }) =>
      get24hChange(currentEthPrice),
    ),
    TE.map(({ currentEthPrice, price24Change }) => ({
      usd: currentEthPrice.ethusd,
      usd24hChange: price24Change,
    })),
    TE.chainFirstIOK((ethStats) => () => {
      ethStatsCache.set("eth-stats", ethStats);
    }),
  );

class CacheMissError extends Error {}
type GetEthStatsError = Get24hAgoPriceError | CacheMissError;

export const getEthStats = (): TE.TaskEither<GetEthStatsError, EthStats> =>
  pipe(getCachedEthStats, O.map(TE.right), O.getOrElse(makeEthStats));

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
        WHERE mined_at >= now() - ${intervalSqlMapNext[timeframe]}::interval
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
