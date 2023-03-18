import * as DateFns from "date-fns";
import { coingekcoLimitQueue } from "./contracts/metadata/coingecko.js";
import { etherscanNameTagQueue } from "./contracts/metadata/etherscan.js";
import { openseaContractQueue } from "./contracts/metadata/opensea.js";
import { twitterProfileQueue } from "./contracts/metadata/twitter.js";
import { web3Queue } from "./contracts/metadata/web3.js";
import * as Etherscan from "./etherscan.js";
import * as Log from "./log.js";

let lastLogQueueSizeTimestamp = new Date();
export const logQueueSizes = () => {
  const secondsSinceLastReport = DateFns.differenceInSeconds(
    new Date(),
    lastLogQueueSizeTimestamp,
  );

  if (secondsSinceLastReport >= 30) {
    lastLogQueueSizeTimestamp = new Date();
    Log.debug(`etherscan meta title queue size: ${etherscanNameTagQueue.size}`);
    Log.debug(`opensea metadata queue size: ${openseaContractQueue.size}`);
    Log.debug(`on chain name queue size: ${web3Queue.size}`);
    Log.debug(`twitter profile queue size: ${twitterProfileQueue.size}`);
    Log.debug(`etherscan api queue size: ${Etherscan.apiQueue.size}`);
    Log.debug(`coingecko meta queue size: ${coingekcoLimitQueue.size}`);
  }
};
