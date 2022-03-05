import * as DateFns from "date-fns";
import PQueue from "p-queue";
import { sql } from "../db.js";
import * as DefiLlama from "../defi_llama.js";
import * as Duration from "../duration.js";
import { RateLimitError } from "../errors.js";
import * as Etherscan from "../etherscan.js";
import * as FetchAlt from "../fetch_alt.js";
import { A, B, E, flow, O, pipe, T, TAlt, TE, TO } from "../fp.js";
import { LeaderboardEntries, LeaderboardEntry } from "../leaderboards.js";
import * as Log from "../log.js";
import * as Opensea from "../opensea.js";
import * as PerformanceMetrics from "../performance_metrics.js";
import * as Twitter from "../twitter.js";
import * as Contracts from "./contracts.js";
import * as ContractsWeb3 from "./web3.js";

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

export const addMetadataForLeaderboards = (
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

export const web3Queue = new PQueue({
  concurrency: 4,
  timeout: Duration.millisFromSeconds(60),
});

const queueWeb3Fetch = <E, A>(task: TE.TaskEither<E, A>) =>
  pipe(
    TE.tryCatch(
      () => web3Queue.add(task),
      () => new TimeoutError(),
    ),
    TE.chainW((e) => (E.isLeft(e) ? TE.left(e.left) : TE.right(e.right))),
  );
const web3LastAttemptMap: Record<string, Date | undefined> = {};

const getShouldAttempt = (
  attemptMap: Partial<Record<string, Date>>,
  address: string,
) =>
  pipe(
    attemptMap[address],
    O.fromNullable,
    O.map(
      (lastAttempted) =>
        DateFns.differenceInHours(new Date(), lastAttempted) < 6,
    ),
    O.getOrElse(() => false),
  );

const handleGetSupportedInterfaceError = (
  address: string,
  interfaceName: string,
  e: ContractsWeb3.NoSupportsInterfaceMethodError | Error,
) =>
  e instanceof ContractsWeb3.NoSupportsInterfaceMethodError
    ? // Not all contracts will implement this method, do nothing.
      T.of(undefined)
    : T.fromIO(() => {
        Log.error(
          `failed to check contract supports interface ${interfaceName} for ${address}`,
          e,
        );
      });

const addWeb3Metadata = (address: string) =>
  pipe(
    ContractsWeb3.getContract(address),
    queueWeb3Fetch,
    TE.chainFirstIOK(() => () => {
      web3LastAttemptMap[address] = new Date();
    }),
    TE.chainTaskK((contract) => {
      const storeErc721Task = pipe(
        ContractsWeb3.getSupportedInterface(contract, "ERC721"),
        TE.matchE(
          (e) => handleGetSupportedInterfaceError(address, "erc721", e),
          (supportsErc_721) =>
            Contracts.setSimpleBooleanColumn(
              "supports_erc_721",
              address,
              supportsErc_721,
            ),
        ),
      );

      const storeErc1155Task = pipe(
        ContractsWeb3.getSupportedInterface(contract, "ERC1155"),
        TE.matchE(
          (e) => handleGetSupportedInterfaceError(address, "erc1155", e),
          (supportsErc_1155) =>
            Contracts.setSimpleBooleanColumn(
              "supports_erc_1155",
              address,
              supportsErc_1155,
            ),
        ),
      );

      const storeNameTask = pipe(
        ContractsWeb3.getName(contract),
        // Contracts may have a NUL byte in their name, which is not safe to store in postgres. We should find a way to store this safely.
        TE.map((name) => name.replaceAll("\x00", "")),
        TE.matchE(
          (e) => {
            if (e instanceof ContractsWeb3.NoNameMethodError) {
              // Not all contracts will implement a name method.
              return Contracts.setSimpleTextColumn("web3_name", address, null);
            }

            Log.error("failed to get web3 contract name", e);
            return T.of(undefined);
          },
          (safeName) =>
            Contracts.setSimpleTextColumn("web3_name", address, safeName),
        ),
      );

      return pipe(
        TAlt.seqTPar(storeErc721Task, storeErc1155Task, storeNameTask),
        T.chain(() => Contracts.updatePreferredMetadata(address)),
      );
    }),
    TE.match(
      (e) => {
        if (e instanceof TimeoutError) {
          // Timeouts are expected.
          return undefined;
        }
        if (e instanceof Etherscan.AbiNotVerifiedError) {
          // Not all contracts we see are verified, that's okay.
          return undefined;
        }

        // Everything else is an error we should look at although we don't want to hold up the entire crawling process and don't have proper error handling higher up yet.
        Log.error("failed to fetch web3 contract", e);
        return undefined;
      },
      (v) => v,
    ),
  );

export const refreshWeb3Metadata = (address: string, forceRefetch = false) =>
  pipe(
    forceRefetch || getShouldAttempt(web3LastAttemptMap, address),
    T.of,
    TAlt.whenTrue(addWeb3Metadata(address)),
  );

type SimilarContract = {
  address: string;
  category: string | null;
  imageUrl: string | null;
  name: string | null;
  twitterHandle: string | null;
};

export const addMetadataFromSimilar = async (
  address: string,
  nameStartsWith: string,
): Promise<void> => {
  Log.debug(
    `attempting to add similar metadata for ${nameStartsWith} - ${address}`,
  );
  const nameStartsWithPlusWildcard = `${nameStartsWith}%`;
  const similarContracts = await sql<SimilarContract[]>`
    SELECT address, category, image_url, name, twitter_handle FROM contracts
    WHERE name ILIKE ${nameStartsWithPlusWildcard}
  `;

  if (similarContracts.length === 0) {
    return;
  }

  Log.debug(
    `found ${similarContracts.length} similar contracts, starting with ${nameStartsWith}`,
  );

  const getFirstKey = (key: keyof SimilarContract): string | undefined =>
    pipe(
      similarContracts,
      A.map((contract) => contract[key]),
      A.map(O.fromNullable),
      A.compact,
      A.head,
      O.toUndefined,
    );

  const category = getFirstKey("category");
  const name = getFirstKey("name");
  const imageUrl = getFirstKey("imageUrl");
  const twitterHandle = getFirstKey("twitterHandle");

  const categoryTask =
    category === undefined
      ? T.of(undefined)
      : Contracts.setSimpleTextColumn("category", address, category);

  const nameTask =
    name === undefined
      ? T.of(undefined)
      : Contracts.setSimpleTextColumn("name", address, name);

  const imageUrlTask =
    imageUrl === undefined
      ? T.of(undefined)
      : Contracts.setSimpleTextColumn("image_url", address, imageUrl);

  const twitterHandleTask =
    twitterHandle === undefined
      ? T.of(undefined)
      : Contracts.setSimpleTextColumn("twitter_handle", address, twitterHandle);

  return pipe(
    TAlt.seqTPar(categoryTask, nameTask, imageUrlTask, twitterHandleTask),
    TAlt.concatAllVoid,
  )();
};

const etherscanNameTagLastAttemptMap: Record<string, Date | undefined> = {};

export const etherscanNameTagQueue = new PQueue({
  concurrency: 4,
  timeout: Duration.millisFromSeconds(60),
});

const addEtherscanNameTag = async (
  address: string,
  forceRefetch = false,
): Promise<void> => {
  const lastAttempted = etherscanNameTagLastAttemptMap[address];

  if (
    forceRefetch === false &&
    lastAttempted !== undefined &&
    DateFns.differenceInHours(new Date(), lastAttempted) < 6
  ) {
    return undefined;
  }

  const name = await etherscanNameTagQueue.add(() =>
    Etherscan.getNameTag(address),
  );

  etherscanNameTagLastAttemptMap[address] = new Date();

  if (name === undefined) {
    return undefined;
  }

  // The name is something like "Compound: cCOMP Token", we attempt to copy metadata from contracts starting with the same name before the colon i.e. /^compound.*/i.
  if (name.indexOf(":") !== -1) {
    const nameStartsWith = name.split(":")[0];
    await addMetadataFromSimilar(address, nameStartsWith);
  }

  await Contracts.setSimpleTextColumn("etherscan_name_tag", address, name)();
  await Contracts.updatePreferredMetadata(address)();
};

// const etherscanMetaTitleLastAttemptMap: Record<string, Date | undefined> = {};

// export const etherscanMetaTitleQueue = new PQueue({
//   concurrency: 2,
//   throwOnTimeout: true,
//   timeout: Duration.millisFromSeconds(60),
// });

// const queueMetaTitleFetch = <E, A>(task: TE.TaskEither<E, A>) =>
//   pipe(
//     TE.tryCatch(
//       () => etherscanMetaTitleQueue.add(task),
//       () => new TimeoutError(),
//     ),
//     TE.chainW((e) => (E.isLeft(e) ? TE.left(e.left) : TE.right(e.right))),
//   );

// const addEtherscanMetaTitle = async (
//   address: string,
//   forceRefetch = false,
// ): Promise<void> => {
//   const lastAttempted = etherscanMetaTitleLastAttemptMap[address];

//   if (
//     forceRefetch === false &&
//     lastAttempted !== undefined &&
//     DateFns.differenceInHours(new Date(), lastAttempted) < 12
//   ) {
//     return undefined;
//   }

//   const name = await queueMetaTitleFetch(Etherscan.getMetaTitle(address))();

//   if (E.isLeft(name)) {
//     if (name.left instanceof TimeoutError) {
//       return;
//     }

//     if (name.left instanceof Etherscan.NoMeaningfulTitleError) {
//       return;
//     }

//     Log.error("etherscan meta title fetch failed", name.left);
//     return;
//   }

//   Log.debug(`found etherscan meta title: ${name.right}, address: ${address}`);

//   etherscanMetaTitleLastAttemptMap[address] = new Date();

//   // The name is something like "Compound: cCOMP Token", we attempt to copy metadata from contracts starting with the same name before the colon i.e. /^compound.*/i.
//   if (name.right.indexOf(":") !== -1) {
//     const nameStartsWith = name.right.split(":")[0];
//     await addMetadataFromSimilar(address, nameStartsWith);
//   }

//   await Contracts.setSimpleTextColumn(
//     "etherscan_name_token",
//     address,
//     name.right,
//   )();
//   await Contracts.updatePreferredMetadata(address)();
// };

const twitterProfileLastAttemptMap: Record<string, Date | undefined> = {};

export const twitterProfileQueue = new PQueue({
  concurrency: 1,
  timeout: Duration.millisFromSeconds(60),
});

export const addTwitterMetadata = async (
  address: string,
  forceRefetch = false,
): Promise<void> => {
  const lastAttempted = twitterProfileLastAttemptMap[address];

  if (
    forceRefetch === false &&
    lastAttempted !== undefined &&
    DateFns.differenceInHours(new Date(), lastAttempted) < 6
  ) {
    return undefined;
  }

  const handle = await Contracts.getTwitterHandle(address)();

  if (O.isNone(handle)) {
    return undefined;
  }

  if (O.isSome(handle) && handle.value.length === 0) {
    Log.warn(`contract twitter handle is empty string, ${address}`);
    return undefined;
  }

  const profile = await twitterProfileQueue.add(
    pipe(
      Twitter.getProfileByHandle(handle.value),
      TE.match(
        (e) => {
          if (
            e instanceof Twitter.InvalidHandleError ||
            e instanceof RateLimitError ||
            e instanceof Twitter.ProfileNotFoundError ||
            (e instanceof FetchAlt.BadResponseError && e.status === 429)
          ) {
            Log.warn(e.message, e);
          } else {
            Log.error(e.message, e);
          }

          return undefined;
        },
        (profile) => profile,
      ),
    ),
  );

  twitterProfileLastAttemptMap[address] = new Date();

  if (profile === undefined) {
    return undefined;
  }

  const imageUrl = Twitter.getProfileImage(profile) ?? null;

  return pipe(
    TAlt.seqTPar(
      Contracts.setSimpleTextColumn("twitter_image_url", address, imageUrl),
      Contracts.setSimpleTextColumn("twitter_name", address, profile.name),
      Contracts.setSimpleTextColumn(
        "twitter_description",
        address,
        profile.description,
      ),
    ),
    T.chainFirstIOK(() => () => {
      Log.debug("updating twitter metadata", {
        name: profile.name,
        description: profile.description,
        imageUrl: profile.profile_image_url,
      });
    }),
    T.chain(() => Contracts.updatePreferredMetadata(address)),
  )();
};

export const openseaContractQueue = new PQueue({
  concurrency: 2,
  timeout: Duration.millisFromSeconds(120),
  throwOnTimeout: true,
});

class TimeoutError extends Error {}

const queueOpenseaFetch = <E, A>(task: TE.TaskEither<E, A>) =>
  pipe(
    TE.tryCatch(
      () => openseaContractQueue.add(task),
      () => new TimeoutError(),
    ),
    TE.chainW((e) => (E.isLeft(e) ? TE.left(e.left) : TE.right(e.right))),
  );

const getShouldFetchOpenseaMetadata = (
  address: string,
  forceRefetch: boolean,
): T.Task<boolean> => {
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
    T.chainFirstIOK((shouldSkip) => () => {
      if (shouldSkip) {
        Log.debug(`opensea metadata, skipping ${address}`);
      }
    }),
    T.chain((shouldSkip) => (shouldSkip ? T.of(false) : T.of(true))),
  );
};

const updateOpenseaMetadataFromContract = (
  address: string,
  contract: Opensea.OpenseaContract,
) =>
  pipe(
    Log.debug(`updating opensea metadata for ${address}`),
    () =>
      TAlt.seqTPar(
        Contracts.setSimpleTextColumn(
          "opensea_twitter_handle",
          address,
          Opensea.getTwitterHandle(contract) ?? null,
        ),
        Contracts.setSimpleTextColumn(
          "opensea_schema_name",
          address,
          Opensea.getSchemaName(contract) ?? null,
        ),
        Contracts.setSimpleTextColumn(
          "opensea_image_url",
          address,
          contract.image_url,
        ),
        Contracts.setSimpleTextColumn("opensea_name", address, contract.name),
      ),
    T.chainFirstIOK(() => () => {
      const twitterHandle = Opensea.getTwitterHandle(contract) ?? null;
      const schemaName = Opensea.getSchemaName(contract) ?? null;
      Log.debug("adding opensea metadata", {
        name: contract.name,
        twitter: twitterHandle,
        schemaName: schemaName,
        imageUrl: contract.image_url,
      });
    }),
    TAlt.concatAllVoid,
  );

const addOpenseaMetadata = (address: string) =>
  pipe(
    Opensea.getContract(address),
    queueOpenseaFetch,
    TE.chainW((contract) =>
      TE.fromTaskK(updateOpenseaMetadataFromContract)(address, contract),
    ),
    TE.match(
      (error) => {
        if (error instanceof TimeoutError) {
          // Timeouts are expected here. The API we rely on is not fast enough to return us all contract metadata we'd like, so we sort by importance and let requests time out.
          return undefined;
        }

        if (error instanceof Opensea.MissingStandardError) {
          // Opensea returns a 406 for some contracts. Not clear why this isn't a 200. We do nothing as a result.
          return undefined;
        }

        if (error instanceof Opensea.NotFoundError) {
          // Opensea doesn't know all contracts.
          return undefined;
        }

        Log.error("failed to get OpenSea metadata", error);
        return undefined;
      },
      () => undefined,
    ),
    T.chain(() =>
      TAlt.seqTPar(
        Contracts.updatePreferredMetadata(address),
        Opensea.setContractLastFetchNow(address),
      ),
    ),
    TAlt.concatAllVoid,
  );

const addOpenseaMetadataMaybe = (
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

const addDefiLlamaMetadata = async (address: string): Promise<void> => {
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
      protocol.twitter ?? null,
    ),
  )();

  Log.debug(
    `updated defi llama metadata, category: ${protocol.category}, twitterHandle: ${protocol.twitter}`,
  );

  await Contracts.updatePreferredMetadata(address)();
};

const addMetadata = (address: string, forceRefetch = false): T.Task<void> =>
  pipe(
    TAlt.seqTPar(
      () => addDefiLlamaMetadata(address),
      // Blockscan started using CloudFlare, returning 503s.
      () => addEtherscanNameTag(address, forceRefetch),
      refreshWeb3Metadata(address, forceRefetch),
      addOpenseaMetadataMaybe(address, forceRefetch),
    ),
    // Adding twitter metadata requires a handle, the previous steps attempt to uncover said handle.
    // Subtly, the updatePreferredMetadata call may uncover a manually set twitter handle.
    T.chain(() => () => addTwitterMetadata(address, forceRefetch)),
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
