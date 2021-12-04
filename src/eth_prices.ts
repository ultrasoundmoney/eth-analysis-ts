import * as DateFns from "date-fns";
import PQueue from "p-queue";
import QuickLRU from "quick-lru";
import * as DateFnsAlt from "./date_fns_alt.js";
import { JsTimestamp } from "./date_fns_alt.js";
import { sql } from "./db.js";
import * as Duration from "./duration.js";
import { EthPrice } from "./etherscan.js";
import * as EthPricesFtx from "./eth_prices_ftx.js";
import * as EthPricesUniswap from "./eth_prices_uniswap.js";
import { E, O, pipe, T, TAlt, TE, TEAlt } from "./fp.js";
import * as Log from "./log.js";
import { intervalSqlMap, LimitedTimeFrame, TimeFrame } from "./time_frame.js";

export type BlockForPrice = {
  timestamp: number;
  number: number;
};

/* ETH price in usd */
type EthUsd = number;

const priceByMinute = new QuickLRU<JsTimestamp, EthUsd>({ maxSize: 5760 });

// Can be simplified if we add historic prices to the eth_prices table.
const getPriceForOlderBlockWithCache = async (
  block: BlockForPrice,
): Promise<EthPrice> => {
  const blockMinedAt = DateFns.fromUnixTime(block.timestamp);
  const roundedTimestamp = DateFns.startOfMinute(blockMinedAt);
  const cPrice = priceByMinute.get(roundedTimestamp.getTime());

  if (cPrice !== undefined) {
    return {
      timestamp: roundedTimestamp,
      ethusd: cPrice,
    };
  }

  Log.debug("ftx price cache miss, fetching 1500 more");
  const prices = await EthPricesFtx.getFtxPrices(
    1500,
    DateFns.addMinutes(blockMinedAt, 1499),
  );

  prices.forEach(([timestamp, price]) => {
    priceByMinute.set(timestamp, price);
  });

  const exactPrice = priceByMinute.get(roundedTimestamp.getTime());
  const earlierPrice = [1, 2, 3, 4, 5].reduce(
    (price: undefined | number, offset) => {
      return (
        price || priceByMinute.get(roundedTimestamp.getTime() - offset * 60000)
      );
    },
    undefined,
  );
  const laterPrice = [1, 2, 3, 4, 5].reduce(
    (price: undefined | number, offset) => {
      return (
        price || priceByMinute.get(roundedTimestamp.getTime() + offset * 60000)
      );
    },
    undefined,
  );

  // Allow a slightly earlier or later price match too. Ftx doesn't return every minute but they return most.
  const price = exactPrice || earlierPrice || laterPrice;

  Log.debug(
    `found eth price, block: ${
      block.number
    }, target timestamp: ${roundedTimestamp.toISOString()}, exact hit: ${priceByMinute.has(
      roundedTimestamp.getTime(),
    )}, price: ${price}`,
  );

  if (price === undefined) {
    throw new Error(
      "successfully fetched ftx prices but target timestamp not among them",
    );
  }

  return {
    timestamp: roundedTimestamp,
    ethusd: price,
  };
};

export const getOldPriceSeqQueue = new PQueue({ concurrency: 1 });

// Execute these sequentially for maximum cache hits.
export const getPriceForOldBlock = (block: BlockForPrice): Promise<EthPrice> =>
  getOldPriceSeqQueue.add(() => getPriceForOlderBlockWithCache(block));

// Odds are the price we're looking for was recently stored. Because of this we keep a cache.
const priceCache = new QuickLRU<number, EthPrice>({
  maxSize: 256,
});

// JS Date rounded to minute precision.
type MinuteDate = Date;

type PriceRow = {
  timestamp: MinuteDate;
  ethusd: number;
};

const toPriceRow = (ethPrice: EthPrice): PriceRow => ({
  timestamp: DateFns.roundToNearestMinutes(ethPrice.timestamp),
  ethusd: ethPrice.ethusd,
});

export const storePrice = (): T.Task<void> =>
  pipe(
    EthPricesUniswap.getMedianEthPrice(),
    T.chainFirstIOK((ethPrice) => () => {
      Log.debug(
        `storing new eth/usdc timestamp: ${ethPrice.timestamp.toISOString()}, price: ${
          ethPrice.ethusd
        }`,
      );
      priceCache.set(ethPrice.timestamp.getTime(), ethPrice);
    }),
    T.chain((uniswapEthPrice) => {
      // Prices can be at most 5 min old.
      const maxPriceAge = Duration.millisFromMinutes(5);
      const isUniPriceWithinLimit =
        DateFnsAlt.millisecondsBetweenAbs(
          uniswapEthPrice.timestamp,
          new Date(),
        ) <= maxPriceAge;
      if (isUniPriceWithinLimit) {
        return T.of(uniswapEthPrice);
      }

      return pipe(
        () => EthPricesFtx.getNearestFtxPrice(maxPriceAge, new Date()),
        T.map((ftxEthPrice) => {
          if (ftxEthPrice === undefined) {
            Log.error(
              "uniswap price too old, fell back to FTX but price was undefined, using uniswap price",
            );
            return uniswapEthPrice;
          }

          const isWithinDistanceLimit =
            DateFnsAlt.millisecondsBetweenAbs(
              new Date(),
              ftxEthPrice.timestamp,
            ) <= maxPriceAge;

          if (isWithinDistanceLimit) {
            return ftxEthPrice;
          }

          Log.error(
            `uniswap and ftx prices more than ${maxPriceAge}s old, failed to find recent price, returning old price`,
          );
          return uniswapEthPrice;
        }),
      );
    }),
    T.chain(
      (ethPrice) => () =>
        sql`
          INSERT INTO eth_prices
            ${sql(toPriceRow(ethPrice))}
          ON CONFLICT DO NOTHING
        `,
    ),
    T.map(() => undefined),
  );

export type HistoricPrice = [JsTimestamp, number];

const getDbEthPrice = (timestamp: Date): TE.TaskEither<string, EthPrice> =>
  pipe(
    () => sql<{ timestamp: Date; ethusd: number }[]>`
      SELECT
        timestamp,
        ethusd
      FROM eth_prices
      ORDER BY ABS(EXTRACT(epoch FROM (timestamp - ${timestamp}::timestamp )))
      LIMIT 1
    `,
    T.map((rows) => rows[0]),
    T.map(O.fromNullable),
    TE.fromTaskOption(() => "eth price table empty"),
  );

export const getEthPrice = (
  timestamp: Date,
  maxAgeMillis: number | undefined = undefined,
): TE.TaskEither<string, EthPrice> => {
  const roundedTimestamp = pipe(timestamp, DateFns.startOfMinute);

  const priceCachedO = pipe(
    roundedTimestamp,
    (dt) => dt.getTime(),
    (jsTimestamp) => priceCache.get(jsTimestamp),
    O.fromNullable,
    TE.fromOption(() => "no eth price in cache"),
  );

  return pipe(
    priceCachedO,
    TE.alt(() => getDbEthPrice(roundedTimestamp)),
    TE.chainEitherK((ethPrice) => {
      if (maxAgeMillis === undefined) {
        return E.right(ethPrice);
      }

      const priceAge = DateFnsAlt.millisecondsBetweenAbs(
        new Date(),
        ethPrice.timestamp,
      );

      if (priceAge > maxAgeMillis) {
        return E.left(
          `timestamp: ${timestamp.toISOString()} and closest eth price are more than ${maxAgeMillis} millis apart`,
        );
      }

      return E.right(ethPrice);
    }),
  );
};

export const get24hAgoPrice = (): TE.TaskEither<string, number> =>
  pipe(
    TE.tryCatch(
      () => sql<{ ethPrice: number }[]>`
        SELECT timestamp, ethusd FROM eth_prices
        ORDER BY ABS(EXTRACT(epoch FROM (timestamp - (NOW() - '1 days'::interval))))
        LIMIT 1
      `,
      String,
    ),
    TE.chain((rows) =>
      pipe(
        rows[0],
        O.fromNullable,
        O.map((row) => row.ethPrice),
        TE.fromOption(() => "get24hAgoPrice, no price in the last 24h"),
      ),
    ),
  );

const get24hChange = (currentPrice: EthPrice): TE.TaskEither<string, number> =>
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

export const getEthStats = (): TE.TaskEither<string, EthStats> => {
  const getCachedPrice = pipe(
    ethStatsCache.get("eth-stats"),
    O.fromNullable,
    TE.fromOption(() => "no eth stats in cache"),
  );

  const makeEthStats = pipe(
    getEthPrice(new Date()),
    TE.chain((latestEthPrice) =>
      TEAlt.seqTParTE(
        TE.of<string, EthPrice>(latestEthPrice),
        get24hChange(latestEthPrice),
      ),
    ),
    TE.map(([latestPrice, price24Change]) => {
      const ethStats = {
        usd: latestPrice.ethusd,
        usd24hChange: price24Change,
      };

      ethStatsCache.set("eth-stats", ethStats);

      return ethStats;
    }),
  );

  return pipe(
    getCachedPrice,
    TE.alt(() => makeEthStats),
  );
};

type AverageEthPrice = {
  m5: number;
  h1: number;
  h24: number;
  d7: number;
  d30: number;
  all: number;
};

type AveragePrice = { ethPriceAverage: number };

const getAllAveragePriceTask = (): T.Task<number> =>
  pipe(
    () => sql<AveragePrice[]>`
      SELECT AVG(eth_price) AS eth_price_average FROM blocks
      WHERE number >= 12965000
    `,
    T.map((rows) => rows[0]?.ethPriceAverage ?? 0),
  );

const getTimeframeAverageTask = (timeframe: LimitedTimeFrame): T.Task<number> =>
  pipe(
    () => sql<AveragePrice[]>`
        SELECT
          AVG(eth_price) AS eth_price_average
        FROM blocks
        WHERE mined_at >= now() - ${intervalSqlMap[timeframe]}::interval
        AND number >= 12965000
      `,
    T.map((rows) => rows[0]?.ethPriceAverage ?? 0),
  );

// Workaround as passing maxAge on .set is broken.
const averagePriceCacheMap: Record<TimeFrame, QuickLRU<string, number>> = {
  "5m": new QuickLRU<string, number>({
    maxSize: 1,
    maxAge: Duration.millisFromSeconds(3),
  }),
  "1h": new QuickLRU<string, number>({
    maxSize: 1,
    maxAge: Duration.millisFromMinutes(2),
  }),
  "24h": new QuickLRU<string, number>({
    maxSize: 1,
    maxAge: Duration.millisFromMinutes(30),
  }),
  "7d": new QuickLRU<string, number>({
    maxSize: 1,
    maxAge: Duration.millisFromMinutes(30),
  }),
  "30d": new QuickLRU<string, number>({
    maxSize: 1,
    maxAge: Duration.millisFromMinutes(30),
  }),
  all: new QuickLRU<string, number>({
    maxSize: 1,
    maxAge: Duration.millisFromMinutes(30),
  }),
};

// const averagePriceCache = new QuickLRU<Timeframe, number>({
//   maxSize: 6,
// });

// const timeFrameCacheDurationMap: Record<Timeframe, number> = {
//   "5m": Duration.milisFromSeconds(3),
//   "1h": Duration.milisFromMinutes(2),
//   "24h": Duration.milisFromMinutes(30),
//   "7d": Duration.milisFromMinutes(30),
//   "30d": Duration.milisFromMinutes(30),
//   all: Duration.milisFromMinutes(30),
// };

const getTimeFrameAverageWithCache = (timeframe: TimeFrame): T.Task<number> =>
  pipe(
    averagePriceCacheMap[timeframe].get(timeframe),
    O.fromNullable,
    O.match(
      () =>
        pipe(
          timeframe === "all"
            ? getAllAveragePriceTask()
            : getTimeframeAverageTask(timeframe),
          T.chainFirstIOK((value) => () => {
            Log.debug(
              `get eth average price for time frame: ${timeframe} cache miss`,
            );
            averagePriceCacheMap[timeframe].set(timeframe, value);
          }),
        ),
      (cValue) =>
        pipe(
          T.of(cValue),
          T.chainFirstIOK(() => () => {
            Log.debug(
              `get eth average price for time frame: ${timeframe} cache hit`,
            );
          }),
        ),
    ),
  );

export const getAveragePrice = (): T.Task<AverageEthPrice> =>
  TAlt.seqSParT({
    m5: getTimeFrameAverageWithCache("5m"),
    h1: getTimeFrameAverageWithCache("1h"),
    h24: getTimeFrameAverageWithCache("24h"),
    d7: getTimeFrameAverageWithCache("7d"),
    d30: getTimeFrameAverageWithCache("30d"),
    all: getTimeFrameAverageWithCache("all"),
  });
