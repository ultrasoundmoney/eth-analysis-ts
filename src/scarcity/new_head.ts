import { BlockDb } from "../blocks/blocks.js";
import * as DateFnsAlt from "../date_fns_alt.js";
import { sql } from "../db.js";
import * as Duration from "../duration.js";
import * as FeeBurn from "../fee_burns.js";
import { serializeBigInt } from "../json.js";
import * as Log from "../log.js";
import * as EthLocked from "./eth_locked.js";
import * as EthStaked from "./eth_staked.js";
import * as EthSupply from "./eth_supply.js";

export const onNewBlock = async (block: BlockDb) => {
  const ethBurned = FeeBurn.getAllFeesBurned().eth;
  const ethLocked = EthLocked.getLastEthLocked();
  const ethStaked = EthStaked.getLastEthStaked();
  const ethSupply = EthSupply.getLastEthSupply();

  if (ethStaked === undefined) {
    Log.error("can't store scarcity, missing eth staked");
    return;
  }

  if (ethLocked === undefined) {
    Log.error("can't store scarcity, missing eth locked");
    return;
  }

  const maxAge = Duration.millisFromMinutes(10);

  const ethStakedAge = DateFnsAlt.millisecondsBetweenAbs(
    new Date(),
    ethStaked.timestamp,
  );
  if (ethStakedAge > maxAge) {
    Log.error("eth staked update too old");
    return;
  }

  const ethLockedAge = DateFnsAlt.millisecondsBetweenAbs(
    new Date(),
    ethLocked.timestamp,
  );
  if (ethLockedAge > maxAge) {
    Log.error("eth locked update too old");
    return;
  }

  const scarcityEngines = {
    engines: [
      {
        name: "staked",
        amount: ethStaked.ethStaked,
        timestamp: new Date("2020-11-03T00:00:00.000Z"),
      },
      {
        name: "locked",
        amount: ethLocked.ethLocked,
        timestamp: new Date("2017-09-02T00:00:00.000Z"),
      },
    ],
    ethSupply: ethSupply,
    ethBurned: ethBurned,
  };

  const insertable = JSON.stringify(scarcityEngines, serializeBigInt);

  await sql`
    INSERT INTO derived_block_stats (
      block_number,
      scarcity
    ) VALUES (
      ${block.number},
      ${insertable}::jsonb
    ) ON CONFLICT (block_number) DO UPDATE
    SET scarcity = excluded.scarcity
  `;

  Log.debug(`store scarcity done for ${block.number}`);
};
