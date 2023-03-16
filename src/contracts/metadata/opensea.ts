import PQueue from "p-queue";
import * as Duration from "../../duration.js";
import { B, flow, O, pipe, T, TAlt, TE, TO } from "../../fp.js";
import * as Log from "../../log.js";
import * as Opensea from "../../opensea.js";
import * as Queues from "../../queues.js";
import * as Contracts from "../contracts.js";

export const openseaContractQueue = new PQueue({
  concurrency: 1,
  throwOnTimeout: true,
  timeout: Duration.millisFromSeconds(120),
});

const getShouldFetchOpenseaMetadata = (
  address: string,
  forceRefetch: boolean,
) => {
  if (forceRefetch) {
    Log.debug(`opensea metadata, force refetch: true, address: ${address}`);
    return T.of(true);
  }

  // NOTE: OpenSea API is slow, we skip previously fetched contracts OpenSea said were not NFTs.
  const shouldSkipSchemaNotNft = pipe(
    Opensea.getSchemaImpliesNft(address),
    // If we've fetched the contract before and OpenSea told us they feel it is not an NFT contract, then skip it.
    T.map(
      flow(
        O.map((schemaImpliesNft) => !schemaImpliesNft),
        // We don't know the OpenSea schema, we can't skip.
        O.getOrElse(() => false),
      ),
    ),
  );

  // If we've fetched the contract recently, skip it, don't fetch.
  const shouldSkipRecentlyFetched = Opensea.getIsRecentlyFetched(address);

  const shouldSkipNotNft = pipe(
    Opensea.getExistingCategory(address),
    TO.match(
      () => false,
      (category) => (category === "nft" ? false : true),
    ),
  );

  const shouldSkip = pipe(
    shouldSkipSchemaNotNft,
    T.chain((skip) => (skip ? T.of(true) : shouldSkipRecentlyFetched)),
    T.chain((skip) => (skip ? T.of(true) : shouldSkipNotNft)),
  );

  return pipe(
    shouldSkip,
    T.chain((shouldSkip) => (shouldSkip ? T.of(false) : T.of(true))),
  );
};

const updateOpenseaMetadataFromContract = (
  address: string,
  contract: Opensea.OpenseaContract,
) =>
  pipe(
    Log.debug(`updating opensea metadata for ${address}`),
    () => {
      const twitterHandle = Opensea.getTwitterHandle(contract);
      const schemaName = Opensea.getSchemaName(contract);
      const name = Opensea.getName(contract);
      const imageUrl = contract.image_url;

      Log.debug("adding opensea metadata", {
        name: name,
        twitter: twitterHandle,
        schemaName: schemaName,
        imageUrl: contract.image_url,
      });

      return { twitterHandle, schemaName, name, imageUrl };
    },
    ({ twitterHandle, imageUrl, name, schemaName }) =>
      TAlt.seqTPar(
        Contracts.setSimpleTextColumn(
          "opensea_twitter_handle",
          address,
          O.toNullable(twitterHandle),
        ),
        Contracts.setSimpleTextColumn(
          "opensea_schema_name",
          address,
          O.toNullable(schemaName),
        ),
        Contracts.setSimpleTextColumn(
          "opensea_image_url",
          address,
          O.toNullable(imageUrl),
        ),
        Contracts.setSimpleTextColumn(
          "opensea_name",
          address,
          O.toNullable(name),
        ),
      ),
    TAlt.concatAllVoid,
  );

const addOpenseaMetadata = (address: string) =>
  pipe(
    Opensea.getContract(address),
    Queues.queueOnQueueWithTimeoutTE(openseaContractQueue),
    TE.chainTaskK((contract) =>
      pipe(
        updateOpenseaMetadataFromContract(address, contract),
        T.chain(() =>
          TAlt.seqTPar(
            Contracts.updatePreferredMetadata(address),
            Opensea.setContractLastFetchNow(address),
          ),
        ),
        TAlt.concatAllVoid,
      ),
    ),
    TE.match(
      (error) => {
        if (error instanceof Opensea.MissingStandardError) {
          // Contracts Opensea doesn't have are fine.
          return undefined;
        }

        if (error instanceof Opensea.NotFoundError) {
          // Opensea doesn't know all contracts.
          return undefined;
        }

        if (error instanceof Queues.TimeoutError) {
          // Timeouts are expected here. The API we rely on is not fast enough to return us all contract metadata we'd like, so we sort by importance and let requests time out.
          Log.debug(
            `twitter metadata request timed out for contract ${address}`,
          );
          return;
        }

        Log.error("failed to get OpenSea metadata", error);
        return undefined;
      },
      () => undefined,
    ),
  );

export const addOpenseaMetadataMaybe = (
  address: string,
  forceRefetch = false,
): T.Task<void> =>
  pipe(
    getShouldFetchOpenseaMetadata(address, forceRefetch),
    T.chain(
      B.match(
        () => T.of(undefined),
        () => addOpenseaMetadata(address),
      ),
    ),
  );
