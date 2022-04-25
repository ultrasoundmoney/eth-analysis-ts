import PQueue from "p-queue";
import * as Duration from "../../duration.js";
import { B, flow, MapS, pipe, TE } from "../../fp.js";
import { getShouldRetry } from "./attempts";

const coingeckoLastAttemptMap: Map<string, Date> = new Map();

export const coingeckoQueue = new PQueue({
  concurrency: 2,
  throwOnTimeout: true,
  timeout: Duration.millisFromSeconds(120),
});

const getContractId = (address: string) =>
  pipe(getContractAddressMap(), TE.map(flow(MapS.lookup(address))));

export const addCoingeckoMetadata = (address: string, forceRefetch: boolean) =>
  pipe(
    getShouldRetry(coingeckoLastAttemptMap, address, forceRefetch),
    B.match(
      () => TE.of(undefined),
      () =>
        pipe(
          getContractId(address),
          TE.chain(updateMetadataForId),
          TE.match(
            (e) => undefined,
            () => undefined,
          ),
        ),
    ),
  );
