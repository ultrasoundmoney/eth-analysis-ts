import * as DateFns from "date-fns";
import _ from "lodash";
import makeEta from "simple-eta";
import * as Blocks from "./blocks/blocks.js";
import { sql } from "./db.js";
import * as EthPricesFtx from "./eth-prices/ftx.js";
import * as EthNode from "./eth_node.js";
import { A, E, O, OAlt, pipe } from "./fp.js";
import * as Log from "./log.js";

await EthNode.connect();

const getLastAnalyzedDate = async (): Promise<Date | undefined> => {
  const rows = await sql`
    SELECT value FROM key_value_store
    WHERE key = 'last-reanalyzed-pricedate'
  `;

  return pipe(
    rows[0]?.value,
    O.fromNullable,
    O.map(DateFns.parseISO),
    O.toUndefined,
  );
};

const setLastAnalyzedDate = async (dt: Date): Promise<void> => {
  await sql`
    INSERT INTO key_value_store
      ${sql({
        key: "last-reanalyzed-pricedate",
        value: JSON.stringify(dt),
      })}
    ON CONFLICT (key) DO UPDATE SET
      value = excluded.value
  `;
};

const getFirstDateToFetch = async () => {
  const block = await EthNode.getBlock(Blocks.londonHardForkBlockNumber);
  return pipe(
    block,
    O.fromNullable,
    OAlt.getOrThrow(
      "failed to fetch london hard fork block for first date to fetch",
    ),
    (block) => DateFns.fromUnixTime(block.timestamp),
    DateFns.startOfMinute,
  );
};

let lastAnalyzedDate = await getLastAnalyzedDate();
let nextDateToFetch =
  lastAnalyzedDate === undefined
    ? await getFirstDateToFetch()
    : DateFns.addMinutes(lastAnalyzedDate, 1);

const minutesToFetchCount = DateFns.differenceInMinutes(
  new Date(),
  nextDateToFetch,
);

Log.debug(`${minutesToFetchCount} minutes to fetch eth price for`);

const eta = makeEta({
  max: minutesToFetchCount,
});
let minutesDone = 0;

const logProgress = _.throttle(() => {
  eta.report(minutesDone);
  const progress = ((minutesDone / minutesToFetchCount) * 100).toFixed(2);
  Log.debug(`eta: ${eta.estimate()}s, progress: ${progress}%`);
}, 8000);

while (nextDateToFetch !== undefined) {
  const ePrices = await EthPricesFtx.getFtxPrices(
    nextDateToFetch,
    DateFns.addMinutes(nextDateToFetch, 1500),
  )();

  if (E.isLeft(ePrices)) {
    throw ePrices.left;
  }

  const insertables = pipe(
    Array.from(ePrices.right.entries()),
    A.map(([timestamp, ethusd]) => ({
      timestamp: new Date(timestamp),
      ethusd,
    })),
  );

  await sql`
    INSERT INTO eth_prices_next
      ${sql(insertables)}
    ON CONFLICT (timestamp) DO UPDATE SET
      ethusd = excluded.ethusd
  `;

  minutesDone = minutesDone + 1500;
  logProgress();

  lastAnalyzedDate = new Date(_.last(Array.from(ePrices.right.keys()))!);

  await setLastAnalyzedDate(lastAnalyzedDate);

  nextDateToFetch = DateFns.addMinutes(lastAnalyzedDate, 1);
}
