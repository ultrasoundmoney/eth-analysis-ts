import { A, O, pipe, T, TAlt } from "../../fp.js";
import { LeaderboardEntries, LeaderboardEntry } from "../../leaderboards.js";
import * as Log from "../../log.js";
import * as PerformanceMetrics from "../../performance_metrics.js";
import * as Contracts from "../contracts.js";
import { addDefiLlamaMetadata } from "./defi_llama.js";
import { addOpenseaMetadataMaybe } from "./opensea.js";
import { refreshWeb3Metadata } from "./web3.js";
import * as Coingecko from "./coingecko.js";

const getAddressFromEntry = (entry: LeaderboardEntry): string | undefined =>
  entry.type === "contract" ? entry.address : undefined;

export const getAddressesForMetadata = (
  leaderboards: LeaderboardEntries | undefined,
): Set<string> => {
  if (leaderboards === undefined || leaderboards === null) {
    Log.error("tried to get addresses for empty leaderboards");
    return new Set();
  }

  return pipe(
    Object.values(leaderboards),
    // We'd like to add metadata longest lasting, to shortest lasting timeframe.
    A.reverse,
    A.flatten,
    A.map(getAddressFromEntry),
    A.map(O.fromNullable),
    A.compact,
    (addresses) => new Set(addresses),
  );
};

export const addMetadata = (address: string, forceRefetch = false) =>
  pipe(
    TAlt.seqTPar(
      Coingecko.checkForMetadata(address, forceRefetch),
      addDefiLlamaMetadata(address),
      // Turn off name tag as blockscan is returning 503 again.
      // Etherscan.checkForMetadata(address, forceRefetch),
      // Turn off opensea metadata, our API key doesn't work anymore.
      // addOpenseaMetadataMaybe(address, forceRefetch),
      refreshWeb3Metadata(address, forceRefetch),
    ),
    // Adding twitter metadata requires a handle, the previous steps attempt to uncover said handle.
    // In addition, any updatePreferredMetadata call may uncover a manually set twitter handle. This should probably be more explicit.
    // Our rate-limits are currently insufficient to handle the number of requests we're making. Disabling this until we can make batch requests.
    // T.chain(() => addTwitterMetadataMaybe(address, forceRefetch)),
    T.chainFirst(() =>
      forceRefetch
        ? Contracts.setSimpleBooleanColumn(
            "force_metadata_fetch",
            address,
            false,
          )
        : T.of(undefined),
    ),
    T.chainFirstIOK(() => () => {
      PerformanceMetrics.logQueueSizes();
    }),
    T.map(() => undefined),
  );

export const addMetadataForAddresses = (
  addresses: string[],
  addressesToRefetch: Set<string>,
): T.Task<void> =>
  pipe(
    addresses,
    T.traverseArray((address) =>
      addMetadata(address, addressesToRefetch.has(address)),
    ),
    T.map(() => undefined),
  );
