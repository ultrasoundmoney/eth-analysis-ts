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
import { O, pipe, seqSParT, seqTParT, T } from "./fp.js";
import * as Log from "./log.js";
import { intervalSqlMap, LimitedTimeframe, Timeframe } from "./timeframe.js";

export type BlockForPrice = {
  timestamp: number;
  number: number;
};

/* ETH price in usd */
type EthUsd = number;

const priceByMinute = new QuickLRU<JsTimestamp, EthUsd>({ maxSize: 5760 });

// Can be simplified if we add historic prices to the eth_prices table.
export const getPriceForOlderBlockWithCache = async (
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
    `found eth price for block: ${
      block.number
    }, target timestamp: ${DateFns.formatISO(
      roundedTimestamp,
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
export const getPriceForOldBlock =
  (block: BlockForPrice): T.Task<EthPrice> =>
  () =>
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

export const findNearestHistoricPrice = (
  orderedPrices: HistoricPrice[],
  target: Date | number,
): HistoricPrice => {
  let nearestPrice = orderedPrices[0];

  for (const price of orderedPrices) {
    const distanceCandidate = Math.abs(
      DateFns.differenceInSeconds(target, price[0]),
    );
    const distanceCurrent = Math.abs(
      DateFns.differenceInSeconds(target, nearestPrice[0]),
    );

    // Prices are ordered from oldest to youngest. If the next candidate is further away, the target has to be older. As coming options are only ever younger, we can stop searching.
    if (distanceCandidate > distanceCurrent) {
      break;
    }

    nearestPrice = price;
    continue;
  }

  return nearestPrice;
};

const getDbEthPrice = (timestamp: Date): T.Task<EthPrice> =>
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
  );

export const getEthPrice = (timestamp: Date): T.Task<EthPrice> =>
  pipe(
    timestamp,
    DateFns.startOfMinute,
    (dt) => dt.getTime(),
    (jsTimestamp) => priceCache.get(jsTimestamp),
    O.fromNullable,
    O.match(
      () =>
        pipe(
          timestamp,
          DateFns.startOfMinute,
          getDbEthPrice,
          T.chainFirstIOK((ethPrice) => () => {
            const formattedTimestamp = pipe(
              timestamp,
              DateFns.startOfMinute,
              DateFns.formatISO,
            );
            Log.debug(
              `get eth price, cache miss, timestamp: ${formattedTimestamp}`,
            );
            priceCache.set(ethPrice.timestamp.getTime(), ethPrice);
          }),
        ),
      (ethPrice) =>
        pipe(
          T.of(ethPrice),
          T.chainFirstIOK(() => () => {
            const formattedTimestamp = pipe(
              timestamp,
              DateFns.startOfMinute,
              DateFns.formatISO,
            );
            Log.debug(
              `get eth price, cache hit, timestamp: ${formattedTimestamp}`,
            );
          }),
        ),
    ),
  );

export const get24hAgoPrice = (): T.Task<number | undefined> =>
  pipe(
    () => sql<{ ethPrice: number }[]>`
      SELECT eth_price FROM blocks
      ORDER BY ABS(EXTRACT(epoch FROM (mined_at - (NOW() - '1 days'::interval))))
      LIMIT 1
    `,
    T.map((rows) => rows[0]?.ethPrice ?? undefined),
  );
const get24hChange = (currentPrice: EthPrice): T.Task<number | undefined> =>
  pipe(
    get24hAgoPrice(),
    T.map((price24hAgo) => {
      if (price24hAgo === undefined) {
        Log.error("failed to find 24h old price in db");
        return undefined;
      }

      return ((currentPrice.ethusd - price24hAgo) / price24hAgo) * 100;
    }),
  );

type EthStats = {
  usd: number;
  usd24hChange: number;
};

const ethStatsCache = new QuickLRU<string, EthStats>({
  maxSize: 1,
  maxAge: Duration.millisFromSeconds(16),
});

export const getEthStats = (): T.Task<EthStats | undefined> => {
  const cStats = ethStatsCache.get("eth-stats");
  if (cStats !== undefined) {
    return T.of(cStats);
  }

  return pipe(
    getEthPrice(new Date()),
    T.chain((latestEthPrice) =>
      seqTParT(T.of(latestEthPrice), get24hChange(latestEthPrice)),
    ),
    T.map(([latestPrice, price24Change]) => {
      if (price24Change === undefined) {
        Log.error("missing 24h change");
        return undefined;
      }

      const ethStats = {
        usd: latestPrice.ethusd,
        usd24hChange: price24Change,
      };

      ethStatsCache.set("eth-stats", ethStats);

      return ethStats;
    }),
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

const getTimeframeAverageTask = (timeframe: LimitedTimeframe): T.Task<number> =>
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
const averagePriceCacheMap: Record<Timeframe, QuickLRU<string, number>> = {
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

const getTimeFrameAverageWithCache = (timeFrame: Timeframe): T.Task<number> =>
  pipe(
    averagePriceCacheMap[timeFrame].get(timeFrame),
    O.fromNullable,
    O.match(
      () =>
        pipe(
          timeFrame === "all"
            ? getAllAveragePriceTask()
            : getTimeframeAverageTask(timeFrame),
          T.chainFirstIOK((value) => () => {
            Log.debug(
              `get eth average price for time frame: ${timeFrame} cache miss`,
            );
            averagePriceCacheMap[timeFrame].set(timeFrame, value);
          }),
        ),
      (cValue) =>
        pipe(
          T.of(cValue),
          T.chainFirstIOK(() => () => {
            Log.debug(
              `get eth average price for time frame: ${timeFrame} cache hit`,
            );
          }),
        ),
    ),
  );

export const getAveragePrice = (): T.Task<AverageEthPrice> =>
  seqSParT({
    m5: getTimeFrameAverageWithCache("5m"),
    h1: getTimeFrameAverageWithCache("1h"),
    h24: getTimeFrameAverageWithCache("24h"),
    d7: getTimeFrameAverageWithCache("7d"),
    d30: getTimeFrameAverageWithCache("30d"),
    all: getTimeFrameAverageWithCache("all"),
  });
