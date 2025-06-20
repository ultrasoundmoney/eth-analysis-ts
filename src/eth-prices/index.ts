import * as DateFns from "date-fns";
import QuickLRU from "quick-lru";
import * as DateFnsAlt from "../date_fns_alt.js";
import { JsTimestamp } from "../date_fns_alt.js";
import { sql, sqlT, sqlTVoid } from "../db.js";
import * as Duration from "../duration.js";
import * as EthPricesFtx from "./ftx.js";
import { E, flow, O, pipe, T, TE } from "../fp.js";
import * as Log from "../log.js";
import * as Store from "./store.js";

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

// Execute this fn sequentially for maximum cache hits.
export const getPriceByDate = (dt: Date) =>
  pipe(
    getCachedPrice(dt),
    TE.fromOption(() => new Error("price not in cache")),
    TE.alt(() => Store.closestPrice(dt)),
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

export const storePrice = (ethPrice: EthPrice) =>
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
      Log.debug(
        `stored price: ${price.ethusd}, date: ${price.timestamp.toISOString()}`,
      );
    }),
    TE.chain((price) => TE.fromTask(storePrice(price))),
  );

export type HistoricPrice = [JsTimestamp, number];

class PriceTooOldError extends Error {}

export const getEthPrice = (
  dt: Date,
  maxAgeMillis: number | undefined = undefined,
) => {
  const dtTrunc = DateFns.startOfMinute(dt);

  return pipe(
    Store.priceByMinute(dtTrunc),
    // We'll often have a price for the closest round minute. This is much
    // faster to lookup. If we don't we can fall back to the slower to fetch
    // closest price.
    TE.alt(() => Store.closestPrice(dtTrunc)),
    TE.chainEitherK((ethPrice) => {
      const priceAge = DateFnsAlt.millisecondsBetweenAbs(
        dtTrunc,
        ethPrice.timestamp,
      );

      return maxAgeMillis === undefined
        ? E.right(ethPrice)
        : priceAge > maxAgeMillis
        ? E.left(
            new PriceTooOldError(
              `timestamp: ${dt.toISOString()} and closest eth price are more than ${maxAgeMillis} millis apart`,
            ),
          )
        : E.right(ethPrice);
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

export type EthStats = {
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
type GetEthStatsError = MissingPriceError | CacheMissError;

export const getEthStats = (): TE.TaskEither<GetEthStatsError, EthStats> =>
  pipe(getCachedEthStats, O.map(TE.right), O.getOrElse(makeEthStats));
