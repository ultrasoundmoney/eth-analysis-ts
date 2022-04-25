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

type IndexPrice = {
  open: number;
  time: JsTimestamp;
};

type IndexPriceResponse = {
  result: IndexPrice[];
  success: boolean;
};

type HistoricPriceMap = Map<JsTimestamp, number>;

// FTX says they allow 6 requests per second. Haven't tested this limit.
export const ftxApiQueue = new PQueue({
  carryoverConcurrencyCount: true,
  concurrency: 2,
  interval: Duration.millisFromSeconds(1),
  intervalCap: 3,
});

const queueApiCall =
  <A>(task: T.Task<A>): T.Task<A> =>
  () =>
    ftxApiQueue.add(task);

const makeEthPriceUrl = (start: Date, end: Date) =>
  urlcat("https://ftx.com/api/indexes/ETH/candles", {
    resolution: 60,
    start_time: DateFns.getUnixTime(start),
    end_time: DateFns.getUnixTime(end),
  });

const retryPolicy = Retry.Monoid.concat(
  Retry.constantDelay(2000),
  Retry.limitRetries(2),
);

export const getFtxPrices = (startDateTime: Date, endDateTime: Date) => {
  const startMinute = DateFns.startOfMinute(startDateTime);
  const endMinute = DateFns.startOfMinute(endDateTime);
  const minutesTotal = DateFns.differenceInMinutes(endMinute, startMinute);

  // FTX returns up to 1500 results per page. We do not support pagination and so cannot return prices for more than 1500 minutes at a time.
  if (minutesTotal > 1500) {
    return TE.left(
      new Error(
        `tried to fetch more prices in one batch (${minutesTotal}) than allowed (1500) from FTX`,
      ),
    );
  }

  return pipe(
    Fetch.fetchWithRetry(makeEthPriceUrl(startMinute, endMinute), undefined, {
      retryPolicy,
    }),
    queueApiCall,
    TE.chainW((res) =>
      pipe(() => res.json() as Promise<IndexPriceResponse>, T.map(E.right)),
    ),
    TE.map(
      (body): HistoricPriceMap =>
        pipe(
          body.result,
          A.reduce(new Map(), (map, indexPrice) =>
            map.set(indexPrice.time, indexPrice.open),
          ),
        ),
    ),
  );
};

export class PriceNotFound extends Error {}

// We might not have the exact price date, as FTX doesn't have every date, it is also possible the start of the minute is so recent, FTX doesn't have a price yet. We accept prices within one minute on either side.
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
    getFtxPrices(start, end),
    TE.chainEitherKW((prices) =>
      pipe(
        getPriceFromMap(prices, targetMinute),
        O.alt(() => getPriceFromMap(prices, targetSubOne)),
        O.alt(() => getPriceFromMap(prices, targetPlusOne)),
        E.fromOption(
          () =>
            new PriceNotFound(
              `FTX returned prices from ${start.toISOString()}, to ${end.toISOString()}, but price for requested date ${dt.toISOString()} is missing`,
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
