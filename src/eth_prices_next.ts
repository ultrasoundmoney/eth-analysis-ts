import * as DateFns from "date-fns";
import * as EthPricesUniswap from "./eth_prices_uniswap.js";
import * as Duration from "./duration.js";
import * as Log from "./log.js";
import { setInterval } from "timers/promises";
import { pipe, T } from "./fp.js";
import { sql } from "./db.js";
import { EthPrice } from "./etherscan.js";

const warnWatermark = 30;
const criticalWatermark = 60;

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

const storePrice = (): T.Task<void> =>
  pipe(
    EthPricesUniswap.getMedianEthPrice(),
    T.chainFirstIOK((ethPrice) => () => {
      Log.debug(
        `storing new eth/usdc timestamp: ${ethPrice.timestamp.toISOString()}, price: ${
          ethPrice.ethusd
        }`,
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

export const storePriceAbortController = new AbortController();

export const continuouslyStorePrice = async () => {
  const intervalIterator = setInterval(
    Duration.millisFromSeconds(10),
    Date.now(),
    { signal: storePriceAbortController.signal },
  );

  let lastRun = new Date();

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  for await (const _ of intervalIterator) {
    const secondsSinceLastRun = DateFns.differenceInSeconds(
      new Date(),
      lastRun,
    );

    if (secondsSinceLastRun >= warnWatermark) {
      Log.warn(
        `store price not keeping up, ${secondsSinceLastRun}s since last price fetch`,
      );
    }

    if (secondsSinceLastRun >= criticalWatermark) {
      Log.error(
        `store price not keeping up, ${secondsSinceLastRun}s since last price fetch`,
      );
    }

    lastRun = new Date();

    await storePrice()();
  }
};

export const getEthPrice = (timestamp: Date): T.Task<EthPrice> =>
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
