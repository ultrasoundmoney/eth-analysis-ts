import * as DateFns from "date-fns";
import PQueue from "p-queue";
import * as Retry from "retry-ts";
import urlcatM from "urlcat";
import { JsTimestamp } from "./date_fns_alt.js";
import * as Duration from "./duration.js";
import * as FetchAlt from "./fetch_alt.js";
import { A, E, O, pipe, T, TE } from "./fp.js";

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
    FetchAlt.fetchWithRetry(
      makeEthPriceUrl(startMinute, endMinute),
      undefined,
      {
        retryPolicy,
      },
    ),
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

export const getPriceByDate = (dt: Date) =>
  pipe(
    {
      start: pipe(dt, DateFns.startOfMinute, (dt) => DateFns.subMinutes(dt, 1)),
      end: DateFns.addMinutes(dt, 1),
    },
    ({ start, end }) =>
      pipe(
        getFtxPrices(start, end),
        TE.chainW((prices) =>
          pipe(
            prices.get(DateFns.getTime(start)),
            O.fromNullable,
            // Sometimes the start of the current minute is too recent for FTX to have a price, so we ask for the past two minutes and return the most recent.
            O.alt(() =>
              pipe(
                prices.get(DateFns.getTime(DateFns.subMinutes(start, 1))),
                O.fromNullable,
              ),
            ),
            E.fromOption(
              () =>
                new PriceNotFound(
                  `FTX returned prices from ${start}, to ${end}, but price for requested date is missing`,
                ),
            ),
            TE.fromEither,
          ),
        ),
        TE.map((price) => ({
          timestamp: start,
          ethusd: price,
        })),
      ),
  );
