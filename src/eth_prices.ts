import * as DateFns from "date-fns";
import PQueue from "p-queue";
import QuickLRU from "quick-lru";
import * as Coingecko from "./coingecko.js";
import { HistoricPrice } from "./coingecko.js";
import * as DateFnsAlt from "./date_fns_alt.js";
import { JsTimestamp } from "./date_fns_alt.js";
import { sql } from "./db.js";
import * as Duration from "./duration.js";
import { EthPrice } from "./etherscan.js";
import { BlockLondon } from "./eth_node.js";
import * as EthPricesEtherscan from "./eth_prices_etherscan.js";
import * as EthPricesFtx from "./eth_prices_ftx.js";
import { O, pipe, seqSParT, seqTParT, T, TE } from "./fp.js";
import * as Log from "./log.js";
import { intervalSqlMap, LimitedTimeframe, Timeframe } from "./timeframe.js";

export type BlockForPrice = {
  timestamp: number;
  number: number;
};

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

const getNearestCoingeckoPrice = async (
  maxDistanceInSeconds: number,
  blockMinedAt: Date,
): Promise<EthPrice | undefined> => {
  const pricesCG = await pipe(
    Coingecko.getPastDayEthPrices(),
    TE.match(
      (e) => {
        Log.error(e.error);
        return undefined;
      },
      (v) => v,
    ),
  )();

  if (pricesCG === undefined) {
    Log.error("failed to fetch coingecko prices for the past day");
    return undefined;
  }

  const oldestCoingeckoPrice = new Date(pricesCG[0][0]);

  if (DateFns.isBefore(blockMinedAt, oldestCoingeckoPrice)) {
    Log.warn("block mined before oldest coingecko 1min eth price");
    return undefined;
  }

  const nearestPrice = findNearestHistoricPrice(pricesCG, blockMinedAt);
  const distance = DateFnsAlt.secondsBetweenAbs(nearestPrice[0], blockMinedAt);

  if (distance > maxDistanceInSeconds) {
    Log.warn(`nearest coingecko price not close enough, diff: ${distance}s`);
    return undefined;
  }

  Log.debug(`found a close enough coingecko price, diff: ${distance}`);

  return {
    timestamp: new Date(nearestPrice[0]),
    ethusd: nearestPrice[1],
  };
};

export const getPriceForBlock = async (
  block: BlockLondon,
): Promise<EthPrice> => {
  const blockMinedAt = DateFns.fromUnixTime(block.timestamp);

  // We only consider a price true for a block if the price was measured at most five minutes from the block being mined in either direction of time.
  const maxPriceAge = 300;

  const priceEtherscan = await EthPricesEtherscan.getNearestEtherscanPrice(
    maxPriceAge,
    blockMinedAt,
  );

  if (priceEtherscan !== undefined) {
    return priceEtherscan;
  }

  Log.warn("etherscan price too old, falling back to ftx");

  const priceFtx = await EthPricesFtx.getNearestFtxPrice(
    maxPriceAge,
    blockMinedAt,
  );

  if (priceFtx !== undefined) {
    return priceFtx;
  }

  Log.warn("ftx price not found or too old, falling back to coingecko");

  const priceCoingecko = await getNearestCoingeckoPrice(
    maxPriceAge,
    blockMinedAt,
  );

  if (priceCoingecko !== undefined) {
    return priceCoingecko;
  }

  Log.error(
    "no price found for block, returning latest price regardless of age",
  );
  return EthPricesEtherscan.getLatestPrice()();
};

/* ETH price in usd */
type EthUsd = number;

const priceByMinute = new QuickLRU<JsTimestamp, EthUsd>({ maxSize: 5760 });

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

export const getPriceForOldBlock =
  (block: BlockForPrice): T.Task<EthPrice> =>
  () =>
    getOldPriceSeqQueue.add(() => getPriceForOlderBlockWithCache(block));

const get24hAgoPrice = (): T.Task<number | undefined> => {
  return pipe(
    () => sql<{ ethPrice: number }[]>`
      SELECT eth_price FROM blocks
      WHERE mined_at > NOW() - interval '1440 minutes'
      AND mined_at < NOW() - interval '1435 minutes'
      ORDER BY number DESC
      LIMIT 1
    `,
    T.map((rows) => rows[0]?.ethPrice ?? undefined),
  );
};

const get24hChange = (): T.Task<number | undefined> =>
  pipe(
    seqTParT(EthPricesEtherscan.getLatestPrice(), get24hAgoPrice()),
    T.map(([latestPrice, price24hAgo]) => {
      if (price24hAgo === undefined) {
        Log.error("failed to find 24h old price in db");
        return undefined;
      }

      return ((latestPrice.ethusd - price24hAgo) / price24hAgo) * 100;
    }),
  );

type EthStats = {
  usd: number;
  usd24hChange: number;
};

const ethStatsCache = new QuickLRU<string, EthStats>({
  maxSize: 1,
  maxAge: Duration.milisFromSeconds(16),
});

export const getEthStats = (): T.Task<EthStats | undefined> => {
  const cStats = ethStatsCache.get("eth-stats");
  if (cStats !== undefined) {
    return T.of(cStats);
  }

  return pipe(
    seqTParT(EthPricesEtherscan.getLatestPrice(), get24hChange()),
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
    maxAge: Duration.milisFromSeconds(3),
  }),
  "1h": new QuickLRU<string, number>({
    maxSize: 1,
    maxAge: Duration.milisFromMinutes(2),
  }),
  "24h": new QuickLRU<string, number>({
    maxSize: 1,
    maxAge: Duration.milisFromMinutes(30),
  }),
  "7d": new QuickLRU<string, number>({
    maxSize: 1,
    maxAge: Duration.milisFromMinutes(30),
  }),
  "30d": new QuickLRU<string, number>({
    maxSize: 1,
    maxAge: Duration.milisFromMinutes(30),
  }),
  all: new QuickLRU<string, number>({
    maxSize: 1,
    maxAge: Duration.milisFromMinutes(30),
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
