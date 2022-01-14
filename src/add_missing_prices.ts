import * as DateFns from "date-fns";
import _ from "lodash";
import makeEta from "simple-eta";
import * as Blocks from "./blocks/blocks.js";
import { sql, sqlTVoid } from "./db.js";
import { EthPrice } from "./eth-prices/eth_prices.js";
import * as EthPricesFtx from "./eth-prices/ftx.js";
import * as EthNode from "./eth_node.js";
import { E, pipe, RA, T, TE } from "./fp.js";
import * as Log from "./log.js";

await EthNode.connect();
const block = await EthNode.getBlock(Blocks.londonHardForkBlockNumber);
let nextDateToCheck = DateFns.startOfMinute(
  DateFns.fromUnixTime(block!.timestamp),
);

const minutesToFetchCount = DateFns.differenceInMinutes(
  new Date(),
  nextDateToCheck,
);

Log.debug(`${minutesToFetchCount} minutes to check eth price for`);

const eta = makeEta({
  max: minutesToFetchCount,
});
let minutesDone = 0;

const logProgress = _.throttle(() => {
  eta.report(minutesDone);
  const progress = ((minutesDone / minutesToFetchCount) * 100).toFixed(2);
  Log.debug(`eta: ${eta.estimate()}s, progress: ${progress}%`);
}, 8000);

while (nextDateToCheck.getTime() <= Date.now()) {
  const minutesBatch = new Array(1000)
    .fill(null)
    .map((_, index) => DateFns.addMinutes(nextDateToCheck, index))
    .filter((dt) => DateFns.isBefore(dt, DateFns.startOfMinute(new Date())));

  if (minutesBatch.length === 0) {
    break;
  }

  const existingTimestamps = await sql<{ timestamp: Date }[]>`
    SELECT timestamp FROM eth_prices
    WHERE timestamp IN (${minutesBatch})
  `.then((rows) => rows.map((row) => row.timestamp));

  const existingTimestampsSet = existingTimestamps.reduce(
    (set, dt) => set.add(dt.getTime()),
    new Set(),
  );

  const missingTimestamps = minutesBatch.filter(
    (dt) => !existingTimestampsSet.has(dt.getTime()),
  );

  if (missingTimestamps.length !== 0) {
    Log.debug(
      `found ${
        missingTimestamps.length
      } missing timestamps, first is ${missingTimestamps[0].toISOString()}`,
    );

    await pipe(
      missingTimestamps,
      T.traverseSeqArray((dt) => pipe(EthPricesFtx.getPriceByDate(dt))),
      T.map((ePrices) => {
        const error = ePrices.find(
          (ePrice) =>
            E.isLeft(ePrice) && !(ePrice instanceof EthPricesFtx.PriceNotFound),
        );

        if (error) {
          return E.left(error);
        }

        return pipe(
          ePrices,
          RA.map(E.getOrElseW(() => null)),
          RA.filter((item): item is EthPrice => item !== null),
          E.right,
        );
      }),
      TE.chainTaskK(
        (prices) =>
          sqlTVoid`
            INSERT INTO eth_prices
              ${sql(prices)}
        `,
      ),
    )();
  }

  const lastInBatch = _.last(minutesBatch)!;
  minutesDone = minutesDone + minutesBatch.length;
  nextDateToCheck = DateFns.addMinutes(lastInBatch, 1);
  logProgress();
}

Log.debug("done!");
