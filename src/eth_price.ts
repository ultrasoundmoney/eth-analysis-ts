import * as Coingecko from "./coingecko.js";
import * as DateFns from "date-fns";
import * as DateFnsAlt from "./date_fns_alt.js";
import * as Duration from "./duration.js";
import * as Etherscan from "./etherscan.js";
import * as Log from "./log.js";
import PQueue from "p-queue";
import QuickLRU from "quick-lru";
import fetch from "node-fetch";
import urlcatM from "urlcat";
import { A, pipe, seqTParT, T, TE } from "./fp.js";
import { BlockLondon } from "./eth_node.js";
import { EthPrice } from "./etherscan.js";
import { HistoricPrice } from "./coingecko.js";
import { JsTimestamp } from "./date_fns_alt.js";
import { sql } from "./db.js";

// NOTE: import is broken somehow, "urlcat is not a function" without.
const urlcat = (urlcatM as unknown as { default: typeof urlcatM }).default;

let latestPrice: EthPrice | undefined = undefined;
let updateLatestPriceInterval: NodeJS.Timer | undefined = undefined;

export const getLatestPrice = (): T.Task<EthPrice> =>
  latestPrice === undefined
    ? pipe(
        setLatestPrice(),
        T.chainFirstIOK(() => () => {
          // On first request start updating periodically.
          if (updateLatestPriceInterval === undefined) {
            updateLatestPriceInterval = setInterval(
              () => setLatestPrice()(),
              Duration.milisFromSeconds(16),
            );
          }
        }),
      )
    : T.of(latestPrice);

const setLatestPrice = (): T.Task<EthPrice> =>
  pipe(
    Etherscan.getEthPrice(),
    TE.match(
      (error) => {
        Log.warn("failed to update eth price from etherscan", { error });

        if (latestPrice === undefined) {
          throw new Error(
            "failed to fetch etherscan eth price, can't initialize eth price",
          );
        }

        if (
          DateFns.differenceInSeconds(new Date(), latestPrice.timestamp) > 300
        ) {
          Log.error(
            "failed to update eth price from etherscan for more than five minutes! calculating with stale price.",
          );
        }

        return latestPrice;
      },
      (ethPrice) => {
        latestPrice = ethPrice;
        return ethPrice;
      },
    ),
  );

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

const getNearestEtherscanPrice = async (
  maxDistanceInSeconds: number,
  blockMinedAt: Date,
): Promise<EthPrice | undefined> => {
  const latestPrice = await getLatestPrice()();
  const distance = DateFnsAlt.secondsBetween(
    blockMinedAt,
    latestPrice.timestamp,
  );
  const isBlockYounger = distance < 0;
  const isWithinDistanceLimit = Math.abs(distance) <= maxDistanceInSeconds;

  if (isBlockYounger) {
    if (!isWithinDistanceLimit) {
      Log.error(
        `block is younger than latest price, diff: ${distance}s, exceeding limit`,
      );
      return undefined;
    }

    Log.debug(
      `block is younger than latest price, diff: ${distance}s, within limit`,
    );
    return latestPrice;
  } else {
    // Block is older than price.
    if (!isWithinDistanceLimit) {
      Log.warn(
        `block is older than latest price, diff: ${distance}s, exceeding limit`,
      );
      return undefined;
    }

    Log.debug(
      `block is older than latest price, diff: ${distance}s, within limit`,
    );
    return latestPrice;
  }
};

// FTX says they allow 6 requests per second. We're not sure yet.
export const ftxApiQueue = new PQueue({
  concurrency: 2,
  interval: Duration.milisFromSeconds(1),
  intervalCap: 3,
});

type IndexPrice = {
  open: number;
  time: JsTimestamp;
};

type IndexPriceResponse = {
  result: IndexPrice[];
  success: boolean;
};

const getFtxPrices = async (
  earlierMinutesToFetch: number,
  timestamp: Date,
): Promise<HistoricPrice[]> => {
  if (earlierMinutesToFetch > 1500) {
    throw new Error("cannot fetch more than 1500 minutes at a time");
  }

  const startTime = pipe(
    timestamp,
    DateFns.startOfMinute,
    // FTX returns up to 1500 results per page. We do not support pagination and so cannot return prices for more than 1500 minutes at a time.
    (dt) => DateFns.subMinutes(dt, earlierMinutesToFetch),
    DateFns.getUnixTime,
  );
  const endTime = pipe(timestamp, DateFns.startOfMinute, DateFns.getUnixTime);

  const url = urlcat("https://ftx.com/api/indexes/ETH/candles", {
    resolution: 60,
    start_time: startTime,
    end_time: endTime,
  });

  const res = await ftxApiQueue.add(() => fetch(url));

  if (res.status !== 200) {
    throw new Error(`failed to fetch ftx prices, status: ${res.status}`);
  }

  const pricesResponse = (await res.json()) as IndexPriceResponse;
  const prices = pricesResponse.result;

  Log.debug("get ftx prices", {
    startTime: DateFns.fromUnixTime(startTime),
    endTime: DateFns.fromUnixTime(endTime),
    pricesCount: prices.length,
    first: prices[0],
    last: prices[prices.length - 1],
  });

  return pipe(
    prices,
    A.map((indexPrice) => [indexPrice.time, indexPrice.open]),
  );
};

const getNearestFtxPrice = async (
  maxDistanceInSeconds: number,
  blockMinedAt: Date,
): Promise<EthPrice | undefined> => {
  const prices = await getFtxPrices(2, blockMinedAt);
  const nearestPrice = findNearestHistoricPrice(prices, blockMinedAt);
  Log.debug("ftx nearest", { prices, blockMinedAt, nearestPrice });
  const distance = DateFnsAlt.secondsBetweenAbs(nearestPrice[0], blockMinedAt);

  if (distance > maxDistanceInSeconds) {
    Log.warn(`nearest ftx price not close enough, diff: ${distance}s`);
    return undefined;
  }

  Log.debug(`found a close enough ftx price, diff: ${distance}`);

  return {
    timestamp: new Date(nearestPrice[0]),
    ethusd: nearestPrice[1],
  };
};

export const getPriceForBlock = async (
  block: BlockLondon,
): Promise<EthPrice | undefined> => {
  const blockMinedAt = DateFns.fromUnixTime(block.timestamp);

  // We only consider a price true for a block if the price was measured at most five minutes from the block being mined in either direction of time.
  const maxPriceAge = 300;

  const priceEtherscan = getNearestEtherscanPrice(maxPriceAge, blockMinedAt);

  if (priceEtherscan !== undefined) {
    return priceEtherscan;
  }

  const priceFtx = await getNearestFtxPrice(maxPriceAge, blockMinedAt);

  if (priceFtx !== undefined) {
    return priceFtx;
  }

  const priceCoingecko = await getNearestCoingeckoPrice(
    maxPriceAge,
    blockMinedAt,
  );

  if (priceCoingecko !== undefined) {
    return priceCoingecko;
  }

  Log.error("no price found for block, returning undefined");
  return undefined;
};

/* ETH price in usd */
type EthUsd = number;

const priceByMinute = new QuickLRU<JsTimestamp, EthUsd>({ maxSize: 5760 });

export const getPriceForOldBlockWithCache = async (
  block: BlockLondon,
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
  const prices = await getFtxPrices(
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

  Log.debug("old block eth price", {
    blockMinedAt: DateFns.fromUnixTime(block.timestamp),
    lookingFor: roundedTimestamp,
    price,
  });

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
  (block: BlockLondon): T.Task<EthPrice> =>
  () =>
    getOldPriceSeqQueue.add(() => getPriceForOldBlockWithCache(block));

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
    seqTParT(getLatestPrice(), get24hAgoPrice()),
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
    seqTParT(getLatestPrice(), get24hChange()),
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
