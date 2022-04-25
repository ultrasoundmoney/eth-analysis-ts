import * as DefiLlama from "../../defi_llama.js";
import { TAlt } from "../../fp.js";
import * as Contracts from "../contracts.js";
import * as Log from "../../log.js";

export const addDefiLlamaMetadata = async (address: string): Promise<void> => {
  // Doing this is fast. We can cache the response for 1h. We therefore do not need a queue.
  const protocols = await DefiLlama.getProtocols();

  if (protocols === undefined) {
    return undefined;
  }

  const protocol = protocols.get(address);

  if (protocol === undefined) {
    return undefined;
  }

  await TAlt.seqTPar(
    Contracts.setSimpleTextColumn(
      "defi_llama_category",
      address,
      protocol.category,
    ),
    Contracts.setSimpleTextColumn(
      "defi_llama_twitter_handle",
      address,
      typeof protocol.twitter === "string" && protocol.twitter.length !== 0
        ? protocol.twitter
        : null,
    ),
  )();

  Log.debug(
    `updated defi llama metadata, category: ${protocol.category}, twitterHandle: ${protocol.twitter}`,
  );

  await Contracts.updatePreferredMetadata(address)();
};
