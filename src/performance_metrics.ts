import * as DateFns from "date-fns";
import { etherscanNameTagQueue } from "./contracts/metadata/etherscan.js";
import { openseaContractQueue } from "./contracts/metadata/opensea.js";
import { twitterProfileQueue } from "./contracts/metadata/twitter.js";
import { web3Queue } from "./contracts/metadata/web3.js";
import * as Etherscan from "./etherscan.js";
import * as Log from "./log.js";
import * as Transactions from "./transactions.js";

const start = new Date();
let lastReport = new Date();

let blocksReceived = 0;
let txrsReceived = 0;
let shouldLogBlockFetchRate = false;

export const setShouldLogBlockFetchRate = (
  newReportPerformance: boolean,
): void => {
  shouldLogBlockFetchRate = newReportPerformance;
};

export const onTxrReceived = () => {
  txrsReceived = txrsReceived + 1;
};

export const onBlockReceived = () => {
  const secondsSinceStart = DateFns.differenceInSeconds(new Date(), start);
  const secondsSinceLastReport = DateFns.differenceInSeconds(
    new Date(),
    lastReport,
  );
  if (secondsSinceLastReport >= 30 && shouldLogBlockFetchRate) {
    lastReport = new Date();
    const blocksRate = (blocksReceived / secondsSinceStart).toFixed(2);
    const txrsRate = (txrsReceived / secondsSinceStart).toFixed(2);
    Log.debug(`block fetch rate: ${blocksRate} b/s`);
    Log.debug(`txr fetch rate: ${txrsRate} txr/s`);
    Log.debug(`txr queue size: ${Transactions.fetchReceiptQueue.size}`);
  }
  blocksReceived = blocksReceived + 1;
};

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
  }
};
