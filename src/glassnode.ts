import * as DateFns from "date-fns";
import { RequestInfo } from "node-fetch";
import urlcatM from "urlcat";
import * as Config from "./config.js";
import * as FetchAlt from "./fetch_alt.js";
import { pipe, TE, TEAlt } from "./fp.js";

// NOTE: import is broken somehow, "urlcat is not a function" without.
const urlcat = (urlcatM as unknown as { default: typeof urlcatM }).default;

const stakedDataUrl = urlcat(
  "https://api.glassnode.com/v1/metrics/eth2/staking_total_volume_sum",
  {
    a: "ETH",
    api_key: Config.getGlassnodeApiKey(),
    c: "NATIVE",
    f: "JSON",
    i: "24h",
    s: DateFns.getUnixTime(new Date("2020-11-03T00:00:00Z")),
    u: DateFns.getUnixTime(new Date()),
  },
);

const fetchData = (url: RequestInfo): TE.TaskEither<Error, unknown> =>
  pipe(
    FetchAlt.fetchWithRetry(url),
    TE.chain((res) => TE.tryCatch(() => res.json(), TEAlt.errorFromUnknown)),
  );

export const getStakedData = () => fetchData(stakedDataUrl);

const circulatingSupplyDataUrl = urlcat(
  "https://api.glassnode.com/v1/metrics/supply/current",
  {
    a: "ETH",
    api_key: Config.getGlassnodeApiKey(),
    c: "NATIVE",
    f: "JSON",
    i: "24h",
    s: DateFns.getUnixTime(new Date("2015-07-30T00:00:00Z")),
    u: DateFns.getUnixTime(new Date()),
  },
);

export const getCirculatingSupplyData = () =>
  fetchData(circulatingSupplyDataUrl);

const ethInSmartContractsDataUrl = urlcat(
  "https://api.glassnode.com/v1/metrics/distribution/supply_contracts",
  {
    a: "ETH",
    api_key: Config.getGlassnodeApiKey(),
    f: "JSON",
    i: "24h",
    s: DateFns.getUnixTime(new Date("2015-08-07T00:00:00Z")),
    u: DateFns.getUnixTime(new Date()),
  },
);

export const getLockedEthData = () => fetchData(ethInSmartContractsDataUrl);
