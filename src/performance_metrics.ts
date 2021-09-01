import { differenceInSeconds } from "date-fns";
import * as Log from "./log.js";
import * as Transactions from "./transactions.js";

const start = new Date();
let lastReport = new Date();

let blocksReceived = 0;
let txrsReceived = 0;
let reportPerformance = false;

export const onBlockReceived = () => {
  const secondsSinceStart = differenceInSeconds(new Date(), start);
  const secondsSinceLastReport = differenceInSeconds(new Date(), lastReport);
  if (secondsSinceLastReport >= 10 && reportPerformance) {
    lastReport = new Date();
    const blocksRate = (blocksReceived / secondsSinceStart).toFixed(2);
    const txrsRate = (txrsReceived / secondsSinceStart).toFixed(2);
    Log.debug(`block fetch rate: ${blocksRate} b/s`);
    Log.debug(`txr fetch rate: ${txrsRate} txr/s`);
    Log.debug(`txr queue size: ${Transactions.txrsPQ.size}`);
  }
  blocksReceived = blocksReceived + 1;
};

export const onTxrReceived = () => {
  txrsReceived = txrsReceived + 1;
};

export const setReportPerformance = (newReportPerformance: boolean): void => {
  reportPerformance = newReportPerformance;
};
