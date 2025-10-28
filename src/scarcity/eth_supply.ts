import { setInterval } from "timers/promises";
import * as Duration from "../duration.js";
import * as Db from "../db.js";
import { E, O, OAlt, pipe, TE } from "../fp.js";
import * as Log from "../log.js";

export type EthSupply = {
  timestamp: Date;
  /** eth supply in Wei */
  ethSupply: bigint;
};

let lastEthSupply: O.Option<EthSupply> = O.none;

const getLatestEthSupplyFromDb = (): TE.TaskEither<Error, bigint> =>
  pipe(
    TE.tryCatch(
      () =>
        Db.sqlT<{ supply: string }[]>`
          SELECT
              supply::TEXT AS supply
          FROM
              eth_supply
          ORDER BY
              timestamp DESC
          LIMIT 1
        `(),
      (e) => new Error(String(e)),
    ),
    TE.chainEitherK((rows) => {
      const supplyText = rows[0]?.supply;
      if (supplyText === undefined || supplyText === null) {
        return E.left(new Error("no rows in eth_supply"));
      }

      try {
        return E.right(BigInt(supplyText));
      } catch (e) {
        return E.left(new Error("invalid bigint in eth_supply.supply"));
      }
    }),
  );

const updateEthSupply = () =>
  pipe(
    getLatestEthSupplyFromDb(),
    TE.chainFirstIOK((ethSupply) => () => {
      Log.debug(`got eth supply from db: ${ethSupply / 10n ** 18n} ETH`);
    }),
    TE.map(
      (ethSupply): EthSupply => ({
        timestamp: new Date(),
        ethSupply,
      }),
    ),
    TE.match(
      (e) => Log.error("failed to update eth supply", e),
      (latestEthSupply) => {
        lastEthSupply = O.some(latestEthSupply);
      },
    ),
  );

export const getLastEthSupply = () =>
  pipe(lastEthSupply, OAlt.getOrThrow("tried to get eth supply before init"));

const intervalIterator = setInterval(Duration.millisFromMinutes(1), Date.now());

const continuouslyUpdate = async () => {
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  for await (const _ of intervalIterator) {
    await updateEthSupply()();
  }
};

export const init = async () => {
  await updateEthSupply()();
  continuouslyUpdate();
};
