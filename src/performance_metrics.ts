import * as Contracts from "./contracts.js";
import * as Etherscan from "./etherscan.js";
import * as Log from "./log.js";
import * as OpenSea from "./opensea.js";
import * as Transactions from "./transactions.js";
import * as Twitter from "./twitter.js";
import { differenceInSeconds } from "date-fns";

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
  const secondsSinceStart = differenceInSeconds(new Date(), start);
  const secondsSinceLastReport = differenceInSeconds(new Date(), lastReport);
  if (secondsSinceLastReport >= 30 && shouldLogBlockFetchRate) {
    lastReport = new Date();
    const blocksRate = (blocksReceived / secondsSinceStart).toFixed(2);
    const txrsRate = (txrsReceived / secondsSinceStart).toFixed(2);
    Log.debug(`block fetch rate: ${blocksRate} b/s`);
    Log.debug(`txr fetch rate: ${txrsRate} txr/s`);
    Log.debug(`txr queue size: ${Transactions.txrsPQ.size}`);
  }
  blocksReceived = blocksReceived + 1;
};

let contractsIdentified = 0;
let lastLogMetadataTimestamp = new Date();
export const onContractIdentified = () => {
  contractsIdentified = contractsIdentified + 1;
  const secondsSinceStart = differenceInSeconds(new Date(), start);
  const secondsSinceLastReport = differenceInSeconds(
    new Date(),
    lastLogMetadataTimestamp,
  );
  if (secondsSinceLastReport >= 30) {
    lastLogMetadataTimestamp = new Date();
    const identifyRate = (contractsIdentified / secondsSinceStart).toFixed(2);
    Log.debug(`contract identify rate: ${identifyRate} c/s`);
  }
};

let lastLogQueueSizeTimestamp = new Date();
export const logQueueSizes = () => {
  const secondsSinceLastReport = differenceInSeconds(
    new Date(),
    lastLogQueueSizeTimestamp,
  );

  if (secondsSinceLastReport >= 30) {
    lastLogQueueSizeTimestamp = new Date();
    Log.debug(
      `fetch metadata queue size: ${Contracts.fetchMetadataQueue.size}`,
    );
    Log.debug(
      `fetch twitter profile queue size: ${Twitter.fetchProfileQueue.size}`,
    );
    Log.debug(
      `fetch opensea contract queue size: ${OpenSea.fetchContractQueue.size}`,
    );
    Log.debug(
      `fetch etherscan token title queue size: ${Etherscan.fetchTokenTitleQueue.size}`,
    );
  }
};
