import { BlockDb } from "../blocks/blocks.js";
import * as DateFnsAlt from "../date_fns_alt.js";
import { sql, sqlNotifyT, sqlT } from "../db.js";
import * as Duration from "../duration.js";
import * as FeeBurn from "../fee_burns.js";
import { A, flow, O, OAlt, pipe, T, TO } from "../fp.js";
import { serializeBigInt } from "../json.js";
import * as Log from "../log.js";
import * as EthLocked from "./eth_locked.js";
import * as EthStaked from "./eth_staked.js";
import * as EthSupply from "./eth_supply.js";

export type ScarcityCache = {
  engines: {
    burned: {
      amount: bigint;
      name: string;
      startedOn: Date;
    };
    locked: {
      amount: number;
      name: string;
      startedOn: Date;
    };
    staked: {
      amount: bigint;
      name: string;
      startedOn: Date;
    };
  };
  ethSupply: bigint;
  number: number;
};

export const scarcityCacheKey = "scarcity-cache-key";

export const updateScarcityCache = async (block: BlockDb) => {
  const ethBurned = FeeBurn.getAllFeesBurned().eth;
  const ethLocked = await pipe(
    EthLocked.getLastEthLocked(),
    TO.getOrElseW(() => T.of(undefined)),
  )();
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

  const ethSupplyAge = DateFnsAlt.millisecondsBetweenAbs(
    new Date(),
    ethSupply.timestamp,
  );
  if (ethSupplyAge > Duration.millisFromMinutes(10)) {
    Log.error("eth supply update too old to calculate scarcity");
    return;
  }

  const scarcity: ScarcityCache = {
    engines: {
      burned: {
        amount: ethBurned,
        name: "burned",
        startedOn: new Date("2021-08-05T12:33:42.000Z"),
      },
      locked: {
        amount: ethLocked.ethLocked,
        name: "locked",
        startedOn: new Date("2017-09-02T00:00:00.000Z"),
      },
      staked: {
        amount: ethStaked.ethStaked,
        name: "staked",
        startedOn: new Date("2020-11-03T00:00:00.000Z"),
      },
    },
    ethSupply: ethSupply.ethSupply,
    number: block.number,
  };

  return pipe(
    sqlT`
      INSERT INTO key_value_store (
        key, value
      ) VALUES (
        ${scarcityCacheKey},
        ${JSON.stringify(scarcity, serializeBigInt)}::json
      ) ON CONFLICT key DO UPDATE SET
        value = excluded.value
    `,
    T.chain(() =>
      // Update scarcity caches about once every 10 blocks
      block.number % 10 === 0
        ? sqlNotifyT("cache-update", scarcityCacheKey)
        : T.of(undefined),
    ),
  )();
};

export const getScarcityCache = () =>
  pipe(
    sqlT<{ value: ScarcityCache }[]>`
      SELECT value FROM key_value_store
      WHERE key = ${scarcityCacheKey}
    `,
    T.map(flow((rows) => rows[0]?.value, O.fromNullable)),
  );
