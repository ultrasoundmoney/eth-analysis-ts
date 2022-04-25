import * as DefiLlama from "../../defi_llama.js";
import { E, flow, MapS, pipe, T, TAlt, TE } from "../../fp.js";
import * as Log from "../../log.js";
import * as Contracts from "../contracts.js";

class UnknownContractError extends Error {}

export const addDefiLlamaMetadata = (address: string) =>
  pipe(
    DefiLlama.getProtocols(),
    TE.chainEitherKW(
      flow(
        MapS.lookup(address),
        E.fromOption(() => new UnknownContractError()),
      ),
    ),
    TE.chainTaskK((protocol) =>
      pipe(
        TAlt.seqTPar(
          Contracts.setSimpleTextColumn(
            "defi_llama_category",
            address,
            protocol.category,
          ),
          Contracts.setSimpleTextColumn(
            "defi_llama_twitter_handle",
            address,
            typeof protocol.twitter === "string" &&
              protocol.twitter.length !== 0
              ? protocol.twitter
              : null,
          ),
        ),
        T.chainFirstIOK(() =>
          Log.debugIO(
            `updated defi llama metadata, category: ${protocol.category}, twitterHandle: ${protocol.twitter}`,
          ),
        ),
        T.chain(() => Contracts.updatePreferredMetadata(address)),
      ),
    ),
    TE.match(
      (e) => {
        if (e instanceof UnknownContractError) {
          // Skip silently
          return;
        }

        Log.error(
          `failed to get defi llama metadata for contract ${address}`,
          e,
        );
      },
      (): void => undefined,
    ),
  );
