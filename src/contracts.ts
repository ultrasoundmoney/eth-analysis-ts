import * as Log from "./log.js";
import * as PerformanceMetrics from "./performance_metrics.js";
import * as T from "fp-ts/lib/Task.js";
import * as Twitter from "./twitter.js";
import A from "fp-ts/lib/Array.js";
import PQueue from "p-queue";
import ProgressBar from "progress";
import fetch from "node-fetch";
import { differenceInDays, differenceInHours } from "date-fns";
import { getEtherscanToken } from "./config.js";
import { parseHTML } from "linkedom";
import { pipe } from "fp-ts/lib/function.js";
import { sql } from "./db.js";
import { web3 } from "./eth_node.js";
import { delay } from "./delay.js";

export const fetchEtherscanName = async (
  address: string,
): Promise<string | undefined> => {
  const html = await fetch(`https://blockscan.com/address/${address}`).then(
    (res) => {
      if (res.status !== 200) {
        throw new Error(
          `bad request trying to fetch etherscan name, status: ${res.status}`,
        );
      }
      return res.text();
    },
  );

  const { document } = parseHTML(html);
  const etherscanPublicName = document.querySelector(".badge-secondary") as {
    innerText: string;
  } | null;

  return etherscanPublicName?.innerText;
};

const contractNameFetchQueue = new PQueue({
  concurrency: 1,
  interval: 1000,
  intervalCap: 1,
});

export const getContractNameFetchedLongAgo = ({
  lastNameFetchAt,
}: {
  lastNameFetchAt: Date | null;
}): boolean =>
  lastNameFetchAt === null ||
  differenceInDays(new Date(), lastNameFetchAt) >= 3;

export const fetchMissingContractNames = async () => {
  const namelessContracts = await sql<
    { address: string; name: null; lastNameFetchAt: Date | null }[]
  >`
    SELECT address, last_name_fetch_at FROM contracts
    WHERE contracts.name IS NULL`;

  Log.info(`found ${namelessContracts.length} contracts without a name`);

  const contractsToFetch = namelessContracts.filter(
    getContractNameFetchedLongAgo,
  );

  Log.info(
    `there are ${contractsToFetch.length} contracts we haven't recently attempted to fetch a name for`,
  );

  const storeNameFound = (address: string, name: string) => sql`
    UPDATE contracts
    SET name = ${name},
        last_name_fetch_at = now()
    WHERE address = ${address}
  `;

  const storeNoNameFound = (address: string) => sql`
    UPDATE contracts
    SET last_name_fetch_at = now()
    WHERE address = ${address}
  `;

  const nameFetchIntervalId = setInterval(() => {
    Log.debug(
      `names left to fetch for ${contractNameFetchQueue.size} contract addresses`,
    );
  }, 8000);

  let bar: ProgressBar | undefined = undefined;
  if (process.env.SHOW_PROGRESS) {
    bar = new ProgressBar("[:bar] :rate/s :percent :etas", {
      total: contractsToFetch.length,
    });
  }

  await contractNameFetchQueue.addAll(
    contractsToFetch.map(
      ({ address }) =>
        () =>
          fetchEtherscanName(address)
            .then((name) =>
              name === undefined
                ? storeNoNameFound(address)
                : storeNameFound(address, name),
            )
            .then(() => bar?.tick()),
    ),
  );

  clearInterval(nameFetchIntervalId);
};

export const addContractMetadataQueue = new PQueue({
  concurrency: 2,
  intervalCap: 3,
  interval: 2000,
});

const updateContractLastMetadataFetchNow = (address: string): Promise<void> =>
  sql`
    UPDATE contracts
    SET last_metadata_fetch_at = NOW()
    WHERE address = ${address}
  `.then(() => undefined);

const updateContractMetadata = (
  address: string,
  name: string | null,
  imageUrl: string | null,
): Promise<void> =>
  sql`
    UPDATE contracts
    SET
      last_metadata_fetch_at = NOW(),
      name = ${name},
      image_url = ${imageUrl}
    WHERE address = ${address}
  `.then(() => undefined);

const getContractMetadata = (
  address: string,
): Promise<{
  name: string | undefined;
  lastMetadataFetchAt: Date | undefined;
  twitterHandle: string | undefined;
}> =>
  sql<
    {
      name: string | null;
      twitterHandle: string | null;
      lastMetadataFetchAt: Date | null;
    }[]
  >`
    SELECT name, twitter_handle, last_metadata_fetch_at FROM contracts
    WHERE address = ${address}
  `.then((rows) => ({
    name: rows[0]?.name ?? undefined,
    lastMetadataFetchAt: rows[0]?.lastMetadataFetchAt ?? undefined,
    twitterHandle: rows[0]?.twitterHandle ?? undefined,
  }));

const addContractMetadata = async (address: string): Promise<void> => {
  const {
    name: currentName,
    lastMetadataFetchAt,
    twitterHandle,
  } = await getContractMetadata(address);

  if (
    lastMetadataFetchAt !== undefined &&
    differenceInHours(new Date(), lastMetadataFetchAt) < 3
  ) {
    // Don't attempt to fetch contract names more than once every three hours.
    return;
  }

  let name;
  if (typeof currentName === "string") {
    // Don't overwrite existing names.
    name = currentName;
  } else {
    const abi = await getAbi(address);
    if (abi === undefined) {
      // No contract source yet.
      return updateContractLastMetadataFetchNow(address);
    }

    const contract = new web3!.eth.Contract(abi as any, address);
    const hasNameMethod = contract.methods["name"] !== undefined;

    if (hasNameMethod) {
      name = await contract.methods.name().call();
    } else {
      name = null;
    }
  }

  PerformanceMetrics.onContractIdentified();

  let imageUrl = null;
  if (twitterHandle !== undefined) {
    imageUrl = (await Twitter.getImageUrl(twitterHandle)) ?? null;
  }
  await updateContractMetadata(address, name, imageUrl);
};

export const addContractsMetadata = (addresses: string[]): Promise<void[]> =>
  addContractMetadataQueue.addAll(
    addresses.map((address) => () => addContractMetadata(address)),
  );

export const storeContracts = (addresses: string[]): T.Task<void> => {
  if (addresses.length === 0) {
    return T.of(undefined);
  }

  return pipe(
    addresses,
    A.map((address) => ({ address })),
    (addresses) => () =>
      sql`
        INSERT INTO contracts
        ${sql(addresses, "address")}
        ON CONFLICT DO NOTHING
      `,
    T.map(() => undefined),
  );
};

export const getAbi = async (address: string, retries = 3): Promise<string> => {
  const res = await fetch(
    `https://api.etherscan.io/api?module=contract&action=getabi&address=${address}&apikey=${getEtherscanToken()}`,
  );

  if ((res.status === 502 || res.status === 503) && retries !== 0) {
    await delay(500);
    return getAbi(address, retries - 1);
  }

  if (res.status !== 200) {
    throw new Error(`get abi failed with status ${res.status}`);
  }

  const body = await (res.json() as Promise<{
    status: "0" | "1";
    result: string;
  }>);
  return body.status === "1" ? JSON.parse(body.result) : undefined;
};
