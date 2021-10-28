import * as DateFns from "date-fns";
import * as Duration from "./duration.js";
import * as Log from "./log.js";
import PQueue from "p-queue";
import fetch from "node-fetch";
import { A, D, E, pipe } from "./fp.js";

const DefiLlamaProtocol = pipe(
  D.struct({
    id: D.string,
    name: D.string,
    address: D.nullable(D.string),
    symbol: D.nullable(D.string),
    url: D.string,
    description: D.nullable(D.string),
    chain: D.string,
    logo: D.nullable(D.string),
    audits: D.nullable(D.string),
    audit_note: D.nullable(D.string),
    gecko_id: D.nullable(D.string),
    cmcId: D.nullable(D.string),
    category: D.nullable(D.string),
    chains: D.array(D.string),
    module: D.string,
    twitter: D.nullable(D.string),
    slug: D.string,
    tvl: D.number,
    chainTvls: D.UnknownRecord,
    change_1h: D.nullable(D.number),
    change_1d: D.nullable(D.number),
    change_7d: D.nullable(D.number),
  }),
  D.intersect(
    D.partial({
      oracles: D.array(D.string),
      fdv: D.number,
      audit_links: D.union(D.string, D.array(D.string)),
      mcap: D.number,
    }),
  ),
);

type DefiLlamaProtocol = D.TypeOf<typeof DefiLlamaProtocol>;

const DefiLlamaProtocols = D.array(DefiLlamaProtocol);

type DefiLlamaProtocols = D.TypeOf<typeof DefiLlamaProtocols>;

export type DefiLlamaProtocolMap = Map<string, DefiLlamaProtocol>;

let protocolsLastFetched: Date | undefined = undefined;

let cachedProtocols: DefiLlamaProtocolMap | undefined = undefined;

export const fetchProtocolsQueue = new PQueue({
  concurrency: 1,
});

const getProtocolsWithCache = async (): Promise<
  DefiLlamaProtocolMap | undefined
> => {
  const shouldRefetch =
    protocolsLastFetched === undefined ||
    DateFns.differenceInSeconds(new Date(), protocolsLastFetched) >
      Duration.secondsFromHours(1);

  if (cachedProtocols !== undefined && !shouldRefetch) {
    return cachedProtocols;
  }

  const res = await fetch("https://api.llama.fi/protocols");

  if (res.status !== 200) {
    Log.error(`fetch defi llama protocols bad response: ${res.status}`);
    return undefined;
  }

  protocolsLastFetched = new Date();

  const protocols = (await res.json()) as DefiLlamaProtocol[];
  const protocolMap = pipe(
    protocols,
    DefiLlamaProtocols.decode,
    E.match(
      (e) => {
        Log.error(D.draw(e));
        return undefined;
      },
      (protocols) =>
        pipe(
          protocols,
          A.reduce(new Map(), (map, protocol) =>
            map.set(protocol.address, protocol),
          ),
        ),
    ),
  );

  cachedProtocols = protocolMap;

  return protocolMap;
};

// In order to fetch protocols once and return the cached map otherwise we need to execute serially.
export const getProtocols = (): Promise<DefiLlamaProtocolMap | undefined> =>
  fetchProtocolsQueue.add(getProtocolsWithCache);
