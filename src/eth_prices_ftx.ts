import * as DateFns from "date-fns";
import fetch from "node-fetch";
import PQueue from "p-queue";
import urlcatM from "urlcat";
import * as DateFnsAlt from "./date_fns_alt.js";
import { JsTimestamp } from "./date_fns_alt.js";
import * as Duration from "./duration.js";
import { EthPrice } from "./etherscan.js";
import * as EthPrices from "./eth_prices.js";
import { HistoricPrice } from "./eth_prices.js";
import { A, pipe } from "./fp.js";
import * as Log from "./log.js";

// NOTE: import is broken somehow, "urlcat is not a function" without.
const urlcat = (urlcatM as unknown as { default: typeof urlcatM }).default;

type IndexPrice = {
  open: number;
  time: JsTimestamp;
};

type IndexPriceResponse = {
  result: IndexPrice[];
  success: boolean;
};

// FTX says they allow 6 requests per second. We're not sure yet.
export const ftxApiQueue = new PQueue({
  concurrency: 2,
  interval: Duration.millisFromSeconds(1),
  intervalCap: 3,
});

export const getFtxPrices = async (
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

  return pipe(
    prices,
    A.map((indexPrice) => [indexPrice.time, indexPrice.open]),
  );
};

const findNearestHistoricPrice = (
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

export const getNearestFtxPrice = async (
  maxDistanceInSeconds: number,
  timestamp: Date,
): Promise<EthPrice | undefined> => {
  const prices = await getFtxPrices(2, timestamp);
  const nearestPrice = findNearestHistoricPrice(prices, timestamp);
  Log.debug("ftx nearest", { prices, blockMinedAt: timestamp, nearestPrice });
  const distance = DateFnsAlt.secondsBetweenAbs(nearestPrice[0], timestamp);

  if (distance > maxDistanceInSeconds) {
    Log.warn(`nearest ftx price not close enough, diff: ${distance}s`);
    return undefined;
  }

  Log.debug(`found a close enough ftx price, diff: ${distance}s`);

  return {
    timestamp: new Date(nearestPrice[0]),
    ethusd: nearestPrice[1],
  };
};
