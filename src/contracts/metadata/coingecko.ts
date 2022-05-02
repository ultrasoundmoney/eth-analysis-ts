import * as DateFns from "date-fns";
import PQueue from "p-queue";
import QuickLRU from "quick-lru";
import * as Coingecko from "../../coingecko.js";
import * as Db from "../../db.js";
import * as Duration from "../../duration.js";
import { A, E, flow, MapS, O, pipe, T, TAlt, TE } from "../../fp.js";
import * as Log from "../../log.js";
import * as Queues from "../../queues.js";
import {
  CoinId,
  getCoinMaps,
} from "../../total-value-secured/total_value_secured.js";
import * as Contracts from "../contracts.js";

type LastAttempt = {
  date: O.Option<Date>;
  totalAttempts: number;
};

const getLastAttempt = (address: string): T.Task<LastAttempt> =>
  pipe(
    Db.sqlT<{ date: Date | null; totalAttempts: number }[]>`
      SELECT
        coingecko_last_fetch AS date,
        coingecko_total_attempts AS total_attempts
      FROM contracts
      WHERE address = ${address}
    `,
    T.map(
      flow(
        A.head,
        O.map((raw) => ({
          date: pipe(raw.date, O.fromNullable),
          totalAttempts: raw.totalAttempts,
        })),
        O.getOrElseW(() => ({
          date: O.none,
          totalAttempts: 0,
        })),
      ),
    ),
  );

const setContractLastAttemptToNow = (address: string, totalAttempts: number) =>
  Db.sqlTVoid`
    UPDATE contracts
    SET
      coingecko_last_fetch = NOW(),
      coingecko_total_attempts = ${totalAttempts}
    WHERE address = ${address}
  `;

const waitInMinutes = Duration.millisFromMinutes(8);

// We use an exponential backoff here.
const getIsPastBackoff = (attempt: LastAttempt) =>
  pipe(
    attempt.date,
    O.map((date) =>
      pipe(
        DateFns.addMilliseconds(
          date,
          waitInMinutes * 2 ** (attempt.totalAttempts - 1),
        ),
        (backoffPoint) => DateFns.isPast(backoffPoint),
      ),
    ),
    O.getOrElseW(() => true),
  );

class UnknownContractError extends Error {}

export const coingeckoQueue = new PQueue({
  concurrency: 2,
  throwOnTimeout: true,
  timeout: Duration.millisFromSeconds(120),
});

type ContractCoinIdMap = Map<string, CoinId>;

const contractCoinIdMapCacheKey = "contract-coin-id-map-cache-key" as const;

const contractCoinIdMapCache = new QuickLRU<
  typeof contractCoinIdMapCacheKey,
  ContractCoinIdMap
>({
  maxAge: Duration.millisFromHours(1),
  maxSize: 1,
});

const getCachedContractCoinIdMap = () =>
  pipe(contractCoinIdMapCache.get(contractCoinIdMapCacheKey), O.fromNullable);

const setCachedContractCoinIdMap =
  (contractCoinIdMap: ContractCoinIdMap) => () => {
    contractCoinIdMapCache.set(contractCoinIdMapCacheKey, contractCoinIdMap);
  };

const seqQueue = new PQueue({ concurrency: 1 });

const getContractCoinIdMapWithCache = () =>
  pipe(
    getCachedContractCoinIdMap(),
    O.match(
      () =>
        pipe(
          getCoinMaps(),
          TE.map((maps) =>
            pipe(
              Array.from(maps.onEthOnly),
              A.reduce(new Map<string, CoinId>(), (map, [coinId, address]) =>
                map.set(address, coinId),
              ),
              (halfMap) =>
                pipe(
                  Array.from(maps.onEthAndOthers),
                  A.reduce(halfMap, (map, [coinId, address]) =>
                    map.set(address, coinId),
                  ),
                ),
            ),
          ),
          TE.chainFirstIOK(setCachedContractCoinIdMap),
        ),
      TE.of,
    ),
  );

const getContractId = (address: string) =>
  pipe(
    getContractCoinIdMapWithCache(),
    Queues.queueOnQueue(seqQueue),
    TE.chainEitherKW(
      flow(
        MapS.lookup(address),
        E.fromOption(() => new UnknownContractError()),
      ),
    ),
  );

export const coingekcoLimitQueue = new PQueue({
  carryoverConcurrencyCount: true,
  concurrency: 2,
  interval: Duration.millisFromSeconds(10),
  intervalCap: 3,
  throwOnTimeout: true,
  timeout: Duration.millisFromMinutes(1),
});

const addMetadata = (address: string) =>
  pipe(
    getContractId(address),
    TE.chainW(Coingecko.getCoin),
    Queues.queueOnQueueWithTimeoutThrown(coingekcoLimitQueue),
    TE.chainTaskK((metadata) =>
      pipe(
        TAlt.seqTPar(
          Contracts.setSimpleTextColumn(
            "coingecko_name",
            address,
            metadata.name,
          ),
          Db.sqlTVoid`
              UPDATE contracts
              SET coingecko_categories = ${
                metadata.categories === null
                  ? null
                  : Db.array(metadata.categories)
              }
              WHERE address = ${address}
            `,
          Contracts.setSimpleTextColumn(
            "coingecko_image_url",
            address,
            metadata.image_url,
          ),
          Contracts.setSimpleTextColumn(
            "coingecko_twitter_handle",
            address,
            metadata.twitter_handle,
          ),
        ),
        T.chainFirstIOK(() =>
          Log.debugIO(
            `updated coingecko metadata, name: ${metadata.name}, twitterHandle: ${metadata.twitter_handle}`,
          ),
        ),
        T.chain(() => Contracts.updatePreferredMetadata(address)),
      ),
    ),
    TE.match(
      (e) => {
        if (e instanceof UnknownContractError) {
          // Silently skip.
          return;
        }

        Log.error(`failed to get coingecko metadata for ${address}`, e);
      },
      (): void => undefined,
    ),
  );

export const checkForMetadata = (address: string, forceRefetch = false) =>
  pipe(
    getLastAttempt(address),
    T.chain((lastAttempt) =>
      TAlt.when(
        forceRefetch || getIsPastBackoff(lastAttempt),
        pipe(
          addMetadata(address),
          T.chain(() =>
            setContractLastAttemptToNow(address, lastAttempt.totalAttempts + 1),
          ),
        ),
      ),
    ),
  );
