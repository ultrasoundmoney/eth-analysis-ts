import * as DateFns from "date-fns";
import PQueue from "p-queue";
import * as Retry from "retry-ts";
import urlcatM from "urlcat";
import { JsTimestamp } from "../date_fns_alt.js";
import * as Duration from "../duration.js";
import * as Fetch from "../fetch.js";
import { A, E, O, pipe, T, TE } from "../fp.js";

// NOTE: import is broken somehow, "urlcat is not a function" without.
const urlcat = (urlcatM as unknown as { default: typeof urlcatM }).default;

type HistoricPriceMap = Map<JsTimestamp, number>;

// Binance says they allow 6 requests per second. Haven't tested this limit.
export const binanceApiQueue = new PQueue({
  carryoverConcurrencyCount: true,
  concurrency: 2,
  interval: Duration.millisFromSeconds(1),
  intervalCap: 3,
});

const queueApiCall =
  <A>(task: T.Task<A>): T.Task<A> =>
  () =>
    binanceApiQueue.add(task);

const makeEthPriceUrl = (start: Date, end: Date) =>
    urlcat("https://data.binance.com/api/v3/klines", {
        symbol: "ETHBUSD", // TODO: Review if we should use another stablecoin or maybe average across them
        interval: "1m",
        startTime: DateFns.getUnixTime(start) * 1000,
        endTime: DateFns.getUnixTime(end) * 1000,
        limit: 1000
    });

const retryPolicy = Retry.Monoid.concat(
  Retry.constantDelay(2000),
  Retry.limitRetries(2),
);

export const getBinancePrices = (startDateTime: Date, endDateTime: Date) => {
  const startMinute = DateFns.startOfMinute(startDateTime);
  const endMinute = DateFns.startOfMinute(endDateTime);
  const minutesTotal = DateFns.differenceInMinutes(endMinute, startMinute);

  // Binance returns up to 1000 results per page. We do not support pagination and so cannot return prices for more than 1000 minutes at a time.
  if (minutesTotal > 1000) {
    return TE.left(
      new Error(
        `tried to fetch more prices in one batch (${minutesTotal}) than allowed (1000) from Binance`,
      ),
    );
  }

  return pipe(
    Fetch.fetchWithRetry(makeEthPriceUrl(startMinute, endMinute), undefined, {
      retryPolicy,
    }),
    queueApiCall,
    TE.chainW((res: any) =>
      pipe(() => res.json() as Promise<any>, T.map(E.right)),
    ),
    TE.map(
      (body): HistoricPriceMap =>
        pipe(
          body,
            A.reduce(new Map(), (map, priceEntry: any) =>
              map.set(priceEntry[0], priceEntry[1]),
          ),
        ),
    ),
  );
};

export class PriceNotFound extends Error {}

// We might not have the exact price date, as Binance doesn't have every date, it is also possible the start of the minute is so recent, Binance doesn't have a price yet. We accept prices within one minute on either side.
export const getPriceByDate = (dt: Date) => {
  const targetMinute = DateFns.startOfMinute(dt);
  const targetSubOne = DateFns.subMinutes(targetMinute, 1);
  const targetPlusOne = DateFns.addMinutes(targetMinute, 1);
  const start = targetSubOne;
  const end = targetPlusOne;

  const getPriceFromMap = (map: HistoricPriceMap, dt: Date) =>
    pipe(
      dt,
      DateFns.getTime,
      (jsTimestamp) => map.get(jsTimestamp),
      O.fromNullable,
    );

  return pipe(
    getBinancePrices(start, end),
    TE.chainEitherKW((prices) =>
      pipe(
        getPriceFromMap(prices, targetMinute),
        O.alt(() => getPriceFromMap(prices, targetSubOne)),
        O.alt(() => getPriceFromMap(prices, targetPlusOne)),
        E.fromOption(
          () =>
            new PriceNotFound(
              `Binance returned prices from ${start.toISOString()}, to ${end.toISOString()}, but price for requested date ${dt.toISOString()} is missing`,
            ),
        ),
      ),
    ),
    TE.map((price) => ({
      timestamp: dt,
      ethusd: price,
    })),
  );
};
