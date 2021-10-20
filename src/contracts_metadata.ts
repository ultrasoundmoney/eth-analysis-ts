import * as Contracts from "./contracts.js";
import * as DateFns from "date-fns";
import * as DefiLlama from "./defi_llama.js";
import * as Duration from "./duration.js";
import * as Etherscan from "./etherscan.js";
import * as Log from "./log.js";
import * as OpenSea from "./opensea.js";
import * as PerformanceMetrics from "./performance_metrics.js";
import * as Twitter from "./twitter.js";
import PQueue from "p-queue";
import { A, O, pipe, seqTParT, T } from "./fp.js";
import { LeaderboardEntries, LeaderboardEntry } from "./leaderboards.js";
import { sql } from "./db.js";

const getAddressFromEntry = (entry: LeaderboardEntry): string | undefined =>
  entry.type === "contract" ? entry.address : undefined;

export const getAddressesForMetadata = (
  leaderboards: LeaderboardEntries,
): Set<string> =>
  pipe(
    Object.values(leaderboards),
    // We'd like to add metadata longest lasting, to shortest lasting timeframe.
    A.reverse,
    A.flatten,
    A.map(getAddressFromEntry),
    A.map(O.fromNullable),
    A.compact,
    (addresses) => new Set(addresses),
  );

export const addMetadataForLeaderboards = (addresses: string[]): T.Task<void> =>
  pipe(
    addresses,
    T.traverseArray(addMetadata),
    T.map(() => undefined),
  );

export const onChainNameQueue = new PQueue({
  concurrency: 4,
  timeout: Duration.milisFromSeconds(60),
});

const onChainLastAttemptMap: Record<string, Date | undefined> = {};

const addOnChainName = async (address: string): Promise<void> => {
  const lastAttempted = onChainLastAttemptMap[address];

  if (
    lastAttempted !== undefined &&
    DateFns.differenceInHours(new Date(), lastAttempted) < 6
  ) {
    return undefined;
  }

  const value = await onChainNameQueue.add(() =>
    Contracts.getOnChainName(address),
  );

  onChainLastAttemptMap[address] = new Date();

  if (value === undefined) {
    return undefined;
  }

  await Contracts.setSimpleColumn("on_chain_name", address, value)();
  await Contracts.updatePreferredMetadata(address)();
};

const etherscanNameTagLastAttemptMap: Record<string, Date | undefined> = {};

export const etherscanNameTagQueue = new PQueue({
  concurrency: 4,
  timeout: Duration.milisFromSeconds(60),
});

type SimilarContract = {
  category: string | null;
  twitterHandle: string | null;
};

const addMetadataFromSimilar = async (
  address: string,
  name: string,
): Promise<void> => {
  Log.debug(`adding metadata from similar contract for ${name} - ${address}`);
  const similarContracts = await sql<SimilarContract[]>`
    SELECT category, twitter_handle FROM contracts
    WHERE name ILIKE '${sql(name)}%'
  `;

  if (similarContracts.length !== 0) {
    Log.debug("found similar contracts", { contracts: similarContracts });
  }

  const category = pipe(
    similarContracts,
    A.map((contract) => contract.category),
    A.map(O.fromNullable),
    A.compact,
    A.head,
    O.toUndefined,
  );

  if (typeof category === "string") {
    await Contracts.setSimpleColumn("manual_category", address, category)();
  }

  const twitterHandle = pipe(
    similarContracts,
    A.map((contract) => contract.twitterHandle),
    A.map(O.fromNullable),
    A.compact,
    A.head,
    O.toUndefined,
  );

  if (typeof twitterHandle === "string") {
    Contracts.setSimpleColumn("manual_twitter_handle", address, twitterHandle);
    addTwitterMetadata(address, twitterHandle);
  }

  return undefined;
};

const addEtherscanNameTag = async (address: string): Promise<void> => {
  const lastAttempted = etherscanNameTagLastAttemptMap[address];

  if (
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

  // The name is something like Compound: cCOMP Token in which case we can try to grab the metadata from the name before the colon.
  if (name.indexOf(":") !== -1) {
    addMetadataFromSimilar(address, name);
  }

  await Contracts.setSimpleColumn("etherscan_name_tag", address, name)();
  await Contracts.updatePreferredMetadata(address)();
};

const twitterProfileLastAttemptMap: Record<string, Date | undefined> = {};

export const twitterProfileQueue = new PQueue({
  concurrency: 2,
  timeout: Duration.milisFromSeconds(60),
});

export const addTwitterMetadata = async (
  address: string,
  handle: string,
): Promise<void> => {
  const lastAttempted = twitterProfileLastAttemptMap[address];

  if (
    lastAttempted !== undefined &&
    DateFns.differenceInHours(new Date(), lastAttempted) < 6
  ) {
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
    seqTParT(
      Contracts.setSimpleColumn("twitter_image_url", address, imageUrl),
      Contracts.setSimpleColumn("twitter_name", address, profile.name),
      Contracts.setSimpleColumn(
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
  timeout: Duration.milisFromSeconds(120),
});

const addOpenseaMetadata = async (address: string): Promise<void> => {
  // Because the OpenSea API is slow, we store the last fetched in the DB instead of memory to make sure we don't repeat ourselves on restarts.
  const lastAttempted = await OpenSea.getContractLastFetch(address);
  if (
    lastAttempted !== undefined &&
    DateFns.differenceInHours(new Date(), lastAttempted) < 6
  ) {
    return undefined;
  }

  // The OpenSea API is mighty slow, we attempt to shorten the request queue by skipping what we can.
  const [existingOpenseaSchemaName] = await sql<
    { openseaSchemaName: string | null }[]
  >`
    SELECT opensea_schema_name
    FROM contracts
    WHERE address = ${address}
  `;

  if (
    existingOpenseaSchemaName !== null &&
    OpenSea.checkSchemaImpliesNft(existingOpenseaSchemaName)
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

  const openseaContract = await openseaContractQueue.add(() =>
    OpenSea.getContract(address),
  );

  await OpenSea.setContractLastFetchNow(address);

  if (openseaContract === undefined) {
    return undefined;
  }

  const twitterHandle = OpenSea.getTwitterHandle(openseaContract);

  if (typeof twitterHandle === "string") {
    // If we have a new handle, we can queue the fetching of twitter metadata.
    addTwitterMetadata(address, twitterHandle);
  }

  const schemaName = OpenSea.getSchemaName(openseaContract) ?? null;

  await seqTParT(
    Contracts.setSimpleColumn(
      "opensea_twitter_handle",
      address,
      twitterHandle ?? null,
    ),
    Contracts.setSimpleColumn("opensea_schema_name", address, schemaName),
    Contracts.setSimpleColumn(
      "opensea_image_url",
      address,
      openseaContract.image_url,
    ),
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

  if (typeof protocol.twitter === "string") {
    // If we have a new handle, we can queue the fetching of twitter metadata.
    addTwitterMetadata(address, protocol.twitter);
  }

  await seqTParT(
    Contracts.setSimpleColumn(
      "defi_llama_category",
      address,
      protocol.category,
    ),
    Contracts.setSimpleColumn(
      "defi_llama_twitter_handle",
      address,
      protocol.twitter,
    ),
  )();
  await Contracts.updatePreferredMetadata(address)();
};

const addMetadata = (address: string): T.Task<void> =>
  pipe(
    T.sequenceArray([
      () => addOnChainName(address),
      () => addEtherscanNameTag(address),
      () => addOpenseaMetadata(address),
      () => addDefiLlamaMetadata(address),
    ]),
    T.chainFirst(() => {
      return async () => {
        const [metadata] = await sql<
          {
            name: string | null;
            category: string | null;
            twitterHandle: string | null;
            imageUrl: string | null;
          }[]
        >`SELECT * FROM contracts WHERE address = ${address}`;
        Log.debug(
          `new metadata address=${address} name=${metadata.name}, category=${metadata.category}, twitterHandle=${metadata.twitterHandle}, imageUrl=${metadata.imageUrl}`,
        );
      };
    }),
    T.chainFirstIOK(() => () => {
      PerformanceMetrics.logQueueSizes();
    }),
    T.map(() => undefined),
  );
