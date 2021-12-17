import * as DateFns from "date-fns";
import * as Blocks from "./blocks/blocks.js";
import * as DateFnsAlt from "./date_fns_alt.js";
import { sql } from "./db.js";
import * as EthPrices from "./eth_prices.js";
import * as Log from "./log.js";

Log.debug("fetching all blocks where eth_price is null");

const blocks = await sql<{ number: number; minedAt: Date }[]>`
  SELECT mined_at, number FROM blocks
  WHERE eth_price IS NULL
  ORDER BY number ASC
`;

let count = 0;

for (const block of blocks) {
  Log.debug(
    `looking for price for block: ${
      block.number
    }, timestamp: ${DateFns.formatISO(block.minedAt)}`,
  );

  const fauxBlock = {
    timestamp: DateFns.getUnixTime(block.minedAt),
    number: block.number,
  };

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const ethPrice = await EthPrices.getPriceForOldBlock(fauxBlock as any);

  Log.debug(
    `found price: ${ethPrice.ethusd}, timestamp: ${DateFns.formatISO(
      ethPrice.timestamp,
    )}`,
  );

  const secondsBetween = DateFnsAlt.secondsBetweenAbs(
    block.minedAt,
    ethPrice.timestamp,
  );

  Log.debug(`time difference: ${secondsBetween}s`);

  if (secondsBetween > 300) {
    throw new Error("price is more than 5min from block");
  }

  await Blocks.setEthPrice(block.number, ethPrice.ethusd)();

  count = count + 1;
  if (count !== 0 && count % 10000 === 0) {
    Log.info("finished adding 10000 prices");
  }
}

sql.end();
