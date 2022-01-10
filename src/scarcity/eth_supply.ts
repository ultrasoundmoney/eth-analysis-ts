import { setInterval } from "timers/promises";
import * as Duration from "../duration.js";
import * as Log from "../log.js";
import * as Etherscan from "../etherscan.js";
import { pipe, TE } from "../fp.js";

type LastEthSupply = {
  timestamp: Date;
  ethSupply: bigint;
};

let lastEthSupply: LastEthSupply | undefined = undefined;

const updateEthSupply = () =>
  pipe(
    Etherscan.getEthSupply(),
    TE.chainFirstIOK((ethSupply) => () => {
      Log.debug(`got eth supply from etherscan: ${ethSupply / 10n ** 18n} ETH`);
    }),
    TE.map((ethSupply) => ({
      timestamp: new Date(),
      ethSupply,
    })),
    TE.match(Log.error, (latestEthSupply) => {
      lastEthSupply = latestEthSupply;
    }),
  );

export const getLastEthSupply = () => {
  return lastEthSupply;
};

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
