import { BlockDb } from "../blocks/blocks.js";
import * as DateFnsAlt from "../date_fns_alt.js";
import { sqlNotifyT, sqlT, sqlTVoid } from "../db.js";
import * as Duration from "../duration.js";
import * as FeeBurn from "../fee_burns.js";
import { E, flow, O, OAlt, pipe, T, TE } from "../fp.js";
import { serializeBigInt } from "../json.js";
import * as Log from "../log.js";
import * as EthLocked from "./eth_locked.js";
import * as EthStaked from "./eth_staked.js";
import * as EthSupply from "./eth_supply.js";

export type Scarcity = {
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

const buildScarcity = (
  block: BlockDb,
  ethLocked: EthLocked.EthLocked,
): E.Either<Error, Scarcity> => {
  const ethBurned = FeeBurn.getAllFeesBurned().eth;
  const ethStaked = EthStaked.getLastEthStaked();
  const ethSupply = EthSupply.getLastEthSupply();

  const ethStakedAge = DateFnsAlt.millisecondsBetweenAbs(
    new Date(),
    ethStaked.timestamp,
  );
  if (ethStakedAge > Duration.millisFromMinutes(10)) {
    return E.left(new Error("eth staked update too old"));
  }

  const ethSupplyAge = DateFnsAlt.millisecondsBetweenAbs(
    new Date(),
    ethSupply.timestamp,
  );
  if (ethSupplyAge > Duration.millisFromMinutes(10)) {
    return E.left(new Error("eth supply update too old to calculate scarcity"));
  }

  return E.right({
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
  });
};

export const updateScarcityCache = (block: BlockDb): T.Task<void> =>
  pipe(
    EthLocked.getLastEthLocked(),
    T.map(OAlt.getOrThrow("can't update scarcity, eth locked is missing")),
    T.map((ethLocked) => buildScarcity(block, ethLocked)),
    TE.chainTaskK(
      (scarcity) =>
        sqlTVoid`
          INSERT INTO key_value_store (
            key, value
          ) VALUES (
            ${scarcityCacheKey},
            ${JSON.stringify(scarcity, serializeBigInt)}::json
          ) ON CONFLICT (key) DO UPDATE SET
          value = excluded.value
        `,
    ),
    TE.chainTaskK(() =>
      // Update scarcity caches about once every 10 blocks
      block.number % 10 === 0
        ? sqlNotifyT("cache-update", scarcityCacheKey)
        : T.of(undefined),
    ),
    TE.match(
      (e) => Log.error("failed to update scarcity", e),
      () => undefined,
    ),
  );

export const getScarcityCache = () =>
  pipe(
    sqlT<{ value: Scarcity }[]>`
      SELECT value FROM key_value_store
      WHERE key = ${scarcityCacheKey}
    `,
    T.map(flow((rows) => rows[0]?.value, O.fromNullable)),
  );
