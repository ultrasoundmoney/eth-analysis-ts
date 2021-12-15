import { setInterval } from "timers/promises";
import * as Duration from "../duration.js";
import * as Log from "../log.js";
import * as Etherscan from "../etherscan.js";

type LastEthSupply = {
  timestamp: Date;
  ethSupply: bigint;
};

let lastEthSupply: LastEthSupply | undefined = undefined;

const updateEthSupply = async () => {
  const ethSupply = await Etherscan.getEthSupply();

  Log.debug(`got eth supply from etherscan: ${ethSupply / 10n ** 18n} ETH`);

  lastEthSupply = {
    timestamp: new Date(),
    ethSupply,
  };
};

export const getLastEthSupply = async () => {
  return lastEthSupply;
};

const intervalIterator = setInterval(Duration.millisFromMinutes(1), Date.now());

const continuouslyUpdate = async () => {
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  for await (const _ of intervalIterator) {
    await updateEthSupply();
  }
};

export const init = async () => {
  await updateEthSupply();

  continuouslyUpdate();
};
