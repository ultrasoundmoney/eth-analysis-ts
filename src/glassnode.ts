import * as DateFns from "date-fns";
import * as UrlSub from "url-sub";
import * as Config from "./config.js";
import * as Fetch from "./fetch.js";
import { D, pipe, TE } from "./fp.js";
import { decodeWithError } from "./decoding.js";

const glassnodeApi = "https://api.glassnode.com";
const makeStakedDataUrl = () =>
  UrlSub.formatUrl(glassnodeApi, "/v1/metrics/eth2/staking_total_volume_sum", {
    a: "ETH",
    api_key: Config.getGlassnodeApiKey(),
    c: "NATIVE",
    f: "JSON",
    i: "24h",
    s: DateFns.getUnixTime(new Date("2020-11-03T00:00:00Z")),
    u: DateFns.getUnixTime(new Date()),
  });

export const getStakedData = () =>
  Fetch.fetchWithRetryJson(makeStakedDataUrl());

const makeCirculatingSupplyDataUrl = () =>
  UrlSub.formatUrl(glassnodeApi, "/v1/metrics/supply/current", {
    a: "ETH",
    api_key: Config.getGlassnodeApiKey(),
    c: "NATIVE",
    f: "JSON",
    i: "24h",
    s: DateFns.getUnixTime(new Date("2015-07-30T00:00:00Z")),
    u: DateFns.getUnixTime(new Date()),
  });

const SupplyData = D.array(
  D.struct({
    t: D.number,
    v: D.number,
  }),
);

export type SupplyData = D.TypeOf<typeof SupplyData>;

export const getCirculatingSupplyData = () =>
  pipe(
    Fetch.fetchWithRetryJson(makeCirculatingSupplyDataUrl()),
    TE.chainEitherKW(decodeWithError(SupplyData)),
  );

const makeEthInSmartContractsDataUrl = () =>
  UrlSub.formatUrl(glassnodeApi, "/v1/metrics/distribution/supply_contracts", {
    a: "ETH",
    api_key: Config.getGlassnodeApiKey(),
    f: "JSON",
    i: "24h",
    s: DateFns.getUnixTime(new Date("2015-08-07T00:00:00Z")),
    u: DateFns.getUnixTime(new Date()),
  });

export const getLockedEthData = () =>
  Fetch.fetchWithRetryJson(makeEthInSmartContractsDataUrl());
