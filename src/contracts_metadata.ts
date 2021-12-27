import * as DateFns from "date-fns";
import PQueue from "p-queue";
import * as Contracts from "./contracts.js";
import * as ContractsWeb3 from "./contracts_web3.js";
import { sql } from "./db.js";
import * as DefiLlama from "./defi_llama.js";
import * as Duration from "./duration.js";
import * as Etherscan from "./etherscan.js";
import { A, E, O, pipe, T, TAlt, TE } from "./fp.js";
import { LeaderboardEntries, LeaderboardEntry } from "./leaderboards.js";
import * as Log from "./log.js";
import * as Opensea from "./opensea.js";
import * as PerformanceMetrics from "./performance_metrics.js";
import * as Twitter from "./twitter.js";

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

const web3LastAttemptMap: Record<string, Date | undefined> = {};

export const addWeb3Metadata = async (
  address: string,
  forceRefetch = false,
): Promise<void> => {
  const lastAttempted = web3LastAttemptMap[address];

  if (
    forceRefetch === false &&
    lastAttempted !== undefined &&
    DateFns.differenceInHours(new Date(), lastAttempted) < 6
  ) {
    return undefined;
  }

  const contractE = await web3Queue.add(() =>
    ContractsWeb3.getContract(address)(),
  );

  // Queue has a timeout and returns undefined when hit.
  if (contractE === undefined) {
    return undefined;
  }

  if (E.isLeft(contractE)) {
    if (contractE.left instanceof Etherscan.AbiNotVerifiedError) {
      // Not all contracts we see are verified, that's okay.
      return undefined;
    }

    // Something else went wrong!
    Log.error(contractE.left);
    return undefined;
  }

  const contract = contractE.right;

  web3LastAttemptMap[address] = new Date();

  const [supportsErc_721, supportsErc_1155, name] = await Promise.all([
    ContractsWeb3.getSupportedInterface(contract, "ERC721"),
    ContractsWeb3.getSupportedInterface(contract, "ERC1155"),
    ContractsWeb3.getName(contract),
  ]);

  // Contracts may have a NUL byte in their name, which is not safe to store in postgres. We should find a way to store this safely.
  const safeName = name?.replaceAll("\x00", "");

  await TAlt.seqTParT(
    safeName === undefined
      ? T.of(undefined)
      : Contracts.setSimpleTextColumn("web3_name", address, safeName),
    supportsErc_721 === undefined
      ? T.of(undefined)
      : Contracts.setSimpleBooleanColumn(
          "supports_erc_721",
          address,
          supportsErc_721,
        ),
    supportsErc_1155 === undefined
      ? T.of(undefined)
      : Contracts.setSimpleBooleanColumn(
          "supports_erc_1155",
          address,
          supportsErc_1155,
        ),
  )();

  await Contracts.updatePreferredMetadata(address)();
};

const etherscanNameTagLastAttemptMap: Record<string, Date | undefined> = {};

export const etherscanNameTagQueue = new PQueue({
  concurrency: 4,
  timeout: Duration.millisFromSeconds(60),
});

type SimilarContract = {
  address: string;
  category: string | null;
  imageUrl: string | null;
  name: string | null;
  twitterHandle: string | null;
};

const addMetadataFromSimilar = async (
  address: string,
  nameStartsWith: string,
): Promise<void> => {
  Log.debug(
    `adding metadata from similar contract for ${nameStartsWith} - ${address}`,
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
    "found similar contracts",
    similarContracts.map((contract) => contract.name),
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
    TAlt.seqTParT(categoryTask, nameTask, imageUrlTask, twitterHandleTask),
    TAlt.concatAllVoid,
  )();
};

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

const twitterProfileLastAttemptMap: Record<string, Date | undefined> = {};

export const twitterProfileQueue = new PQueue({
  concurrency: 2,
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

  if (handle === undefined) {
    return undefined;
  }

  const profile = await twitterProfileQueue.add(() =>
    Twitter.getProfileByHandle(handle),
  );

  twitterProfileLastAttemptMap[address] = new Date();

  if (profile === undefined) {
    return undefined;
  }

  const imageUrl = Twitter.getProfileImage(profile) ?? null;

  return pipe(
    TAlt.seqTParT(
      Contracts.setSimpleTextColumn("twitter_image_url", address, imageUrl),
      Contracts.setSimpleTextColumn("twitter_name", address, profile.name),
      Contracts.setSimpleTextColumn(
        "twitter_description",
        address,
        profile.description,
      ),
    ),
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
    TE.chainW((e) => {
      if (E.isLeft(e)) {
        return TE.left(e.left);
      }

      return TE.right(e.right);
    }),
  );

const addOpenseaMetadata = async (
  address: string,
  forceRefetch = false,
): Promise<void> => {
  // Because the OpenSea API is slow, we store the last fetched in the DB instead of memory to make sure we don't repeat ourselves on restarts.
  const lastAttempted = await Opensea.getContractLastFetch(address);
  if (
    forceRefetch === false &&
    lastAttempted !== undefined &&
    DateFns.differenceInHours(new Date(), lastAttempted) < 6
  ) {
    return undefined;
  }

  // OpenSea API is slow, we attempt to shorten the request queue by skipping what we can.
  const [existingOpenseaSchemaName] = await sql<
    { openseaSchemaName: string | null }[]
  >`
    SELECT opensea_schema_name
    FROM contracts
    WHERE address = ${address}
  `;

  if (
    existingOpenseaSchemaName !== null &&
    Opensea.checkSchemaImpliesNft(existingOpenseaSchemaName)
  ) {
    // OpenSea knows about this contract, and feels its not an NFT contract. We assume they're unlikely to add new information for it.
    return undefined;
  }

  const existingCategory = await sql<{ category: string | null }[]>`
    SELECT category FROM contracts WHERE address = ${address}
  `.then((rows) => rows[0]?.category ?? undefined);

  if (existingCategory !== null && existingCategory !== "nft") {
    // We think this category not to be nft, we assume OpenSea will not have information on it and we skip.
    return undefined;
  }

  const contractE = await pipe(
    Opensea.getContract(address),
    queueOpenseaFetch,
  )();

  await Opensea.setContractLastFetchNow(address);

  if (E.isLeft(contractE)) {
    if (contractE.left instanceof TimeoutError) {
      // Timeouts are expected here. The API we rely on is not fast enough to return us all contract metadata we'd like, so we sort by importance and let requests time out.
      return undefined;
    }
    Log.error(contractE.left);
    return undefined;
  }

  const contract = contractE.right;
  const twitterHandle = Opensea.getTwitterHandle(contract) ?? null;
  const schemaName = Opensea.getSchemaName(contract) ?? null;

  await TAlt.seqTParT(
    Contracts.setSimpleTextColumn(
      "opensea_twitter_handle",
      address,
      twitterHandle,
    ),
    Contracts.setSimpleTextColumn("opensea_schema_name", address, schemaName),
    Contracts.setSimpleTextColumn(
      "opensea_image_url",
      address,
      contract.image_url,
    ),
    Contracts.setSimpleTextColumn("opensea_name", address, contract.name),
  )();

  await Contracts.updatePreferredMetadata(address)();
};

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

  await TAlt.seqTParT(
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
  await Contracts.updatePreferredMetadata(address)();
};

type Metadata = {
  name: string | null;
  category: string | null;
  twitterHandle: string | null;
  imageUrl: string | null;
  supportsErc_721: boolean | null;
  supportsErc_1155: boolean | null;
};
const addMetadata = (address: string, forceRefetch = false): T.Task<void> =>
  pipe(
    TAlt.seqTParT(
      () => addWeb3Metadata(address, forceRefetch),
      () => addEtherscanNameTag(address, forceRefetch),
      () => addOpenseaMetadata(address, forceRefetch),
      () => addDefiLlamaMetadata(address),
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
    T.chainFirst(() => {
      return async () => {
        const [metadata] = await sql<Metadata[]>`
          SELECT * FROM contracts WHERE address = ${address}
        `;
        Log.debug("new metadata", {
          address: address,
          name: metadata.name,
          category: metadata.category,
          twitterHandle: metadata.twitterHandle,
          imageUrl: metadata.imageUrl,
          ERC721: metadata.supportsErc_721,
          ERC1155: metadata.supportsErc_1155,
        });
      };
    }),
    T.chainFirstIOK(() => () => {
      PerformanceMetrics.logQueueSizes();
    }),
    T.map(() => undefined),
  );
