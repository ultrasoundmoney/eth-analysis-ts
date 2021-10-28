import * as Coingecko from "./coingecko.js";
import * as ContractsMetadata from "./contracts_metadata.js";
import * as DefiLlama from "./defi_llama.js";
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

let lastLogQueueSizeTimestamp = new Date();
export const logQueueSizes = () => {
  const secondsSinceLastReport = differenceInSeconds(
    new Date(),
    lastLogQueueSizeTimestamp,
  );

  if (secondsSinceLastReport >= 30) {
    lastLogQueueSizeTimestamp = new Date();
    Log.debug(
      `fetch twitter profile queue size: ${Twitter.fetchProfileQueue.size}`,
    );
    Log.debug(
      `fetch opensea contract queue size: ${OpenSea.fetchContractQueue.size}`,
    );
    Log.debug(
      `fetch etherscan token title queue size: ${Etherscan.fetchTokenTitleQueue.size}`,
    );
    Log.debug(
      `fetch defi llama protocols queue size: ${DefiLlama.fetchProtocolsQueue.size}`,
    );
    Log.debug(
      `opensea metadata queue size: ${ContractsMetadata.openseaContractQueue.size}`,
    );
    Log.debug(`on chain name queue size: ${ContractsMetadata.web3Queue.size}`);
    Log.debug(
      `etherscan name tag queue size: ${ContractsMetadata.etherscanNameTagQueue.size}`,
    );
    // Log.debug(
    //   `etherscan name token queue size: ${ContractsMetadata.etherscanNameTokenQueue.size}`,
    // );
    Log.debug(
      `twitter profile queue size: ${ContractsMetadata.twitterProfileQueue.size}`,
    );
    Log.debug(`coingecko api queue size: ${Coingecko.apiQueue.size}`);
  }
};
