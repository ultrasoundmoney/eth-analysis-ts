import { setInterval } from "timers/promises";
import * as Duration from "../duration.js";
import * as FetchAlt from "../fetch_alt.js";
import * as Log from "../log.js";

const fetchWithRetry = FetchAlt.withRetry(5, 2000, true);

const marketDataEndpoint = `https://data-api.defipulse.com/api/v1/defipulse/api/MarketData?api-key=${process.env.DEFI_PULSE_API_KEY}`;

type LastEthLocked = {
  timestamp: Date;
  ethLocked: number;
};

let lastEthLocked: LastEthLocked | undefined = undefined;

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

const updateEthLocked = async () => {
  Log.debug("getting ETH locked from DefiPulse");
  const res = await fetchWithRetry(marketDataEndpoint);

  if (res.status !== 200) {
    Log.error(`bad response from defi pulse ${res.status}`);
  }

  const marketData = (await res.json()) as MarketData;
  const ethLocked = marketData.All.value.total.ETH.value;

  Log.debug(`got eth locked from defi pulse: ${ethLocked} ETH`);

  lastEthLocked = {
    timestamp: new Date(),
    ethLocked,
  };
};

export const getEthLocked = async () => {
  return lastEthLocked;
};

updateEthLocked();

const intervalIterator = setInterval(Duration.millisFromHours(12), Date.now());

// eslint-disable-next-line @typescript-eslint/no-unused-vars
for await (const _ of intervalIterator) {
  await updateEthLocked();
}
