import { BlockV1 } from "../blocks/blocks.js";
import * as DateFnsAlt from "../date_fns_alt.js";
import { sqlT, sqlTNotify, sqlTVoid } from "../db.js";
import * as Duration from "../duration.js";
import { WeiBI, weiFromGwei } from "../eth_units.js";
import * as Fetch from "../fetch.js";
import { E, flow, O, OAlt, pipe, T, TE } from "../fp.js";
import { serializeBigInt } from "../json.js";
import * as Log from "../log.js";
import * as EthLocked from "./eth_locked.js";
import * as EthStaked from "./eth_staked.js";
import * as EthSupply from "./eth_supply.js";

export type Scarcity = {
  engines: {
    burned: {
      amount: WeiBI;
      name: string;
      startedOn: Date;
    };
    locked: {
      amount: number;
      name: string;
      startedOn: Date;
    };
    staked: {
      amount: WeiBI;
      name: string;
      startedOn: Date;
    };
  };
  ethSupply: bigint;
  number: number;
};

export const scarcityCacheKey = "scarcity-cache-key";

const buildScarcity = (
  block: BlockV1,
  ethInDefi: EthLocked.EthLocked,
  ethBurned: bigint,
): TE.TaskEither<
  Fetch.BadResponseError | Fetch.DecodeJsonError | Fetch.FetchError,
  Scarcity
> =>
  pipe(
    TE.Do,
    TE.apS("ethStaked", EthStaked.getEthStaked()),
    TE.apS("ethSupply", TE.of(EthSupply.getLastEthSupply())),
    TE.bind("ethStakedAge", ({ ethStaked }) =>
      TE.of(
        DateFnsAlt.millisecondsBetweenAbs(
          new Date(),
          new Date(ethStaked.timestamp),
        ),
      ),
    ),
    TE.bind("ethSupplyAge", ({ ethSupply }) =>
      TE.of(DateFnsAlt.millisecondsBetweenAbs(new Date(), ethSupply.timestamp)),
    ),
    TE.chainEitherK(({ ethStaked, ethSupply, ethStakedAge, ethSupplyAge }) => {
      if (ethStakedAge > Duration.millisFromMinutes(10)) {
        return E.left(new Error("eth staked update too old"));
      }

      if (ethSupplyAge > Duration.millisFromMinutes(10)) {
        return E.left(
          new Error("eth supply update too old to calculate scarcity"),
        );
      }

      return E.right({
        engines: {
          burned: {
            amount: ethBurned,
            name: "burned",
            startedOn: new Date("2021-08-05T12:33:42.000Z"),
          },
          locked: {
            amount: ethInDefi.eth,
            name: "locked",
            startedOn: new Date("2017-09-02T00:00:00.000Z"),
          },
          staked: {
            amount: BigInt(weiFromGwei(ethStaked.sum)),
            name: "staked",
            startedOn: new Date("2020-11-03T00:00:00.000Z"),
          },
        },
        ethSupply: ethSupply.ethSupply,
        number: block.number,
      });
    }),
  );

export const updateScarcityCache = (block: BlockV1): T.Task<void> =>
  pipe(
    T.Do,
    T.apS(
      "ethInDefi",
      pipe(
        EthLocked.getLastEthInDefi(),
        T.map(OAlt.getOrThrow("can't update scarcity, eth locked is missing")),
      ),
    ),
    // TODO: replace this with a call to the fees burned module when its fast.
    T.apS(
      "ethBurned",
      pipe(
        sqlT<{ feesBurnedAll: string }[]>`
        SELECT value::jsonb#>'{feeBurns,feesBurnedAll}' AS fees_burned_all FROM key_value_store kvs WHERE key = 'grouped-analysis-1'
      `,

        T.map((rows) => BigInt(rows[0]?.feesBurnedAll)),
      ),
    ),
    T.chain(({ ethBurned, ethInDefi }) =>
      buildScarcity(block, ethInDefi, ethBurned),
    ),
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
        ? sqlTNotify("cache-update", scarcityCacheKey)
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
