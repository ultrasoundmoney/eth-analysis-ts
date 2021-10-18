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

export const addMetadataForLeaderboards = (
  leaderboards: LeaderboardEntries,
): T.Task<void> =>
  pipe(
    leaderboards,
    getAddressesForMetadata,
    (set) => Array.from(set),
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
  const similarContracts = await sql<SimilarContract[]>`
    SELECT category, twitter_handle FROM contracts
    WHERE name ILIKE '${sql(name)}%'
  `;

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

const openseaContractLastAttempMap: Record<string, Date | undefined> = {};

export const openseaContractQueue = new PQueue({
  concurrency: 2,
  timeout: Duration.milisFromSeconds(60),
});

const addOpenseaMetadata = async (address: string): Promise<void> => {
  const lastAttempted = openseaContractLastAttempMap[address];

  if (
    lastAttempted !== undefined &&
    DateFns.differenceInHours(new Date(), lastAttempted) < 6
  ) {
    return undefined;
  }

  const openseaContract = await openseaContractQueue.add(() =>
    OpenSea.getContract(address),
  );

  openseaContractLastAttempMap[address] = new Date();

  if (openseaContract === undefined) {
    return undefined;
  }

  const twitterHandle = OpenSea.getTwitterHandle(openseaContract);

  if (typeof twitterHandle === "string") {
    // If we have a new handle, we can queue the fetching of twitter metadata.
    addTwitterMetadata(address, twitterHandle);
  }

  const category = OpenSea.getCategory(openseaContract) ?? null;

  await seqTParT(
    Contracts.setSimpleColumn(
      "opensea_twitter_handle",
      address,
      twitterHandle ?? null,
    ),
    Contracts.setSimpleColumn("opensea_category", address, category),
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
        Log.info(`done storing metadata for ${metadata.name} - ${address}`);
        Log.debug(
          `new metadata name=${metadata.name}, category=${metadata.category}, twitterHandle=${metadata.twitterHandle}, imageUrl=${metadata.imageUrl}`,
        );
      };
    }),
    T.chainFirstIOK(() => () => {
      PerformanceMetrics.logQueueSizes();
    }),
    T.map(() => undefined),
  );
