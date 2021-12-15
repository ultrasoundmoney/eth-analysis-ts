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

export type ScarcityEngine = {
  amount: number | bigint;
  name: string;
  startedOn: Date;
};

export type Scarcity = {
  engines: ScarcityEngine[];
  ethSupply: bigint;
  number: number;
};

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

  if (ethSupply === undefined) {
    Log.error("can't store scarcity, missing eth supply");
    return;
  }

  const ethStakedAge = DateFnsAlt.millisecondsBetweenAbs(
    new Date(),
    ethStaked.timestamp,
  );
  if (ethStakedAge > Duration.millisFromMinutes(10)) {
    Log.error("eth staked update too old");
    return;
  }

  const ethLockedAge = DateFnsAlt.millisecondsBetweenAbs(
    new Date(),
    ethLocked.timestamp,
  );
  if (ethLockedAge > Duration.millisFromDays(1)) {
    Log.error("eth locked update too old");
    return;
  }

  const ethSupplyAge = DateFnsAlt.millisecondsBetweenAbs(
    new Date(),
    ethSupply.timestamp,
  );
  if (ethSupplyAge > Duration.millisFromMinutes(10)) {
    Log.error("eth supply update too old to calculate scarcity");
  }

  const scarcity: Scarcity = {
    engines: [
      {
        amount: ethStaked.ethStaked,
        name: "staked",
        startedOn: new Date("2020-11-03T00:00:00.000Z"),
      },
      {
        amount: ethLocked.ethLocked,
        name: "locked",
        startedOn: new Date("2017-09-02T00:00:00.000Z"),
      },
      {
        amount: ethBurned,
        name: "burned",
        startedOn: new Date("2021-08-05T12:33:42.000Z"),
      },
    ],
    ethSupply: ethSupply.ethSupply,
    number: block.number,
  };

  const insertable = JSON.stringify(scarcity, serializeBigInt);

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

  Log.debug("done updating scarcity");
};
