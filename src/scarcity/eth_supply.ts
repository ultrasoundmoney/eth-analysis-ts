import { setInterval } from "timers/promises";
import * as Duration from "../duration.js";
import * as Etherscan from "../etherscan.js";
import { O, OAlt, pipe, TE } from "../fp.js";
import * as Log from "../log.js";

export type EthSupply = {
  timestamp: Date;
  ethSupply: bigint;
};

let lastEthSupply: O.Option<EthSupply> = O.none;

const updateEthSupply = () =>
  pipe(
    Etherscan.getEthSupply(),
    TE.chainFirstIOK((ethSupply) => () => {
      Log.debug(`got eth supply from etherscan: ${ethSupply / 10n ** 18n} ETH`);
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
