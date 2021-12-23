import { setInterval } from "timers/promises";
import * as Duration from "../duration.js";
import * as FetchAlt from "../fetch_alt.js";
import * as Log from "../log.js";
import * as Config from "../config.js";

const fetchWithRetry = FetchAlt.withRetry(5, 2000, true);

const marketDataEndpoint = `https://data-api.defipulse.com/api/v1/defipulse/api/MarketData?api-key=${process.env.DEFI_PULSE_API_KEY}`;

type LastEthLocked = {
  timestamp: Date;
  ethLocked: number;
};

let lastEthLocked: LastEthLocked | undefined = undefined;

const storeEthLocked = (ethLocked: number) => {
  lastEthLocked = {
    timestamp: new Date(),
    ethLocked,
  };
};

type MarketData = {
  All: {
    value: {
      total: {
        ETH: {
          value: number;
        };
      };
    };
  };
};

// Uses 5 API credits per call, we have 2000 per month.
const getEthLocked = async (): Promise<number | undefined> => {
  Log.debug("getting ETH locked from DefiPulse");
  const res = await fetchWithRetry(marketDataEndpoint);

  if (res.status === 429) {
    Log.error("defi pulse get eth locked 429");
    return undefined;
  }

  if (res.status !== 200) {
    throw new Error(`bad response from defi pulse ${res.status}`);
  }

  const marketData = (await res.json()) as MarketData;
  const ethLocked = marketData.All.value.total.ETH.value;

  Log.debug(`got eth locked from defi pulse: ${ethLocked} ETH`);

  return ethLocked;
};

export const getLastEthLocked = () => lastEthLocked;

const intervalIterator = setInterval(Duration.millisFromHours(12), Date.now());

export const init = async () => {
  // As we don't have many API credits for this endpoint and services may restart many times during dev, we don't fetch a fresh number during dev.
  if (Config.getEnv() === "prod" || Config.getEnv() === "staging") {
    const ethLocked = await getEthLocked();
    if (ethLocked === undefined) {
      Log.error("failed to store defi pulse eth locked");
      return;
    }
    storeEthLocked(ethLocked);
  } else {
    storeEthLocked(9499823.32579059);
  }

  continuouslyUpdate();
};

const continuouslyUpdate = async () => {
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  for await (const _ of intervalIterator) {
    const ethLocked = await getEthLocked();
    if (ethLocked === undefined) {
      Log.error("failed to store defi pulse eth locked");
      return;
    }
    storeEthLocked(ethLocked);
  }
};
