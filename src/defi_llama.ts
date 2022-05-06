import PQueue from "p-queue";
import QuickLRU from "quick-lru";
import * as Duration from "./duration.js";
import * as Fetch from "./fetch.js";
import { A, D, flow, O, pipe, TE } from "./fp.js";
import * as Queues from "./queues.js";

const DefiLlamaProtocol = pipe(
  D.struct({
    id: D.string,
    name: D.string,
    address: D.nullable(D.string),
    category: D.nullable(D.string),
  }),
  D.intersect(
    D.partial({
      chain: D.nullable(D.string),
      chainTvls: D.nullable(D.UnknownRecord),
      chains: D.array(D.string),
      description: D.nullable(D.string),
      fdv: D.nullable(D.number),
      logo: D.nullable(D.string),
      mcap: D.nullable(D.number),
      slug: D.nullable(D.string),
      symbol: D.nullable(D.string),
      tvl: D.nullable(D.number),
      twitter: D.nullable(D.string),
      url: D.nullable(D.string),
    }),
  ),
);

type DefiLlamaProtocol = D.TypeOf<typeof DefiLlamaProtocol>;

const DefiLlamaProtocols = D.array(DefiLlamaProtocol);

export type DefiLlamaProtocolMap = Map<string, DefiLlamaProtocol>;

const protocolMapCache = new QuickLRU<string, DefiLlamaProtocolMap>({
  maxSize: 1,
  maxAge: Duration.millisFromHours(1),
});

const protocolsCacheKey = "protocols-cache-key";

const getCachedProtocolMap = () =>
  pipe(protocolMapCache.get(protocolsCacheKey), O.fromNullable);

const setCachedProtocolMap = (protocolMap: DefiLlamaProtocolMap) => () => {
  protocolMapCache.set(protocolsCacheKey, protocolMap);
};

const getProtocolsWithCache = () =>
  pipe(
    getCachedProtocolMap(),
    O.match(
      () =>
        pipe(
          Fetch.fetchWithRetryJson("https://api.llama.fi/protocols"),
          TE.chainEitherKW(DefiLlamaProtocols.decode),
          TE.map(
            flow(
              A.filter(
                (
                  protocol,
                ): protocol is DefiLlamaProtocol & { address: string } =>
                  typeof protocol.address === "string",
              ),
              A.reduce(new Map() as DefiLlamaProtocolMap, (map, protocol) =>
                map.set(protocol.address.toLowerCase(), protocol),
              ),
            ),
          ),
          TE.chainFirstIOK(setCachedProtocolMap),
        ),
      (protocolMap) => TE.of(protocolMap),
    ),
  );

// To optimize cache hits we answer sequentially.
const seqQueue = new PQueue({ concurrency: 1 });

export const getProtocols = () =>
  pipe(getProtocolsWithCache(), Queues.queueOnQueue(seqQueue));
