import { setInterval } from "timers/promises";
import * as Duration from "../duration.js";
import { GweiNumber, WeiBI, weiFromGwei } from "../eth_units.js";
import * as Format from "../format.js";
import { O, OAlt, pipe, T, TO } from "../fp.js";
import * as KeyValueStore from "../key_value_store.js";
import * as Log from "../log.js";

export type EthStaked = {
  timestamp: Date;
  ethStaked: WeiBI;
};

type DateTimeString = string;

let lastEthStaked: O.Option<EthStaked> = O.none;

const updateEthStaked = (): T.Task<void> =>
  pipe(
    KeyValueStore.getValue<{
      sum: GweiNumber;
      timestamp: DateTimeString;
      slot: number;
    }>("effective-balance-sum"),
    TO.chainFirstIOK((effectiveBalanceSum) =>
      Log.debugIO(
        `effective balance sum found in db: ${Format.ethFromGwei(
          effectiveBalanceSum.sum,
        )}`,
      ),
    ),
    T.map(
      O.match(
        () => {
          Log.warn("no effective balance sum found in db, skipping update");
        },
        (effectiveBalanceSum) => {
          lastEthStaked = O.some({
            timestamp: new Date(),
            ethStaked: BigInt(weiFromGwei(effectiveBalanceSum.sum)),
          });
        },
      ),
    ),
  );

export const getLastEthStaked = (): EthStaked =>
  pipe(lastEthStaked, OAlt.getOrThrow("tried to get eth staked before init"));

const intervalIterator = setInterval(Duration.millisFromMinutes(1), Date.now());

const continuouslyUpdate = async () => {
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  for await (const _ of intervalIterator) {
    await updateEthStaked()();
  }
};

export const init = async () => {
  await updateEthStaked()();
  continuouslyUpdate();
};
