import * as Blocks from "./blocks.js";
import * as EthPrices from "./eth_prices.js";
import * as DateFns from "date-fns";
import * as DateFnsAlt from "./date_fns_alt.js";
import * as Log from "./log.js";
import { sql } from "./db.js";

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

  const ethPrice = await EthPrices.getPriceForOldBlock(fauxBlock)();

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
