import { differenceInDays, differenceInHours } from "date-fns";
import { parseHTML } from "linkedom";
import fetch from "node-fetch";
import PQueue from "p-queue";
import ProgressBar from "progress";
import { sql } from "./db.js";
import * as Log from "./log.js";
import A from "fp-ts/lib/Array.js";
import { pipe } from "fp-ts/lib/function.js";
import * as T from "fp-ts/lib/Task.js";
import Config from "./config.js";
import { web3 } from "./eth_node.js";

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

// type Contract = {
//   address: string;
//   name: string | null;
//   lastNameFetchAt: Date | null;
// };

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

export const identifyContractQueue = new PQueue({
  concurrency: 4,
  intervalCap: 5,
  interval: 1000,
});

const updateContractLastNameFetchToNow = (address: string): Promise<void> =>
  sql`
    UPDATE contracts
    SET last_name_fetch_at = NOW()
    WHERE address = ${address}
  `.then(() => undefined);

const updateContractName = (address: string, name: string): Promise<void> =>
  sql`
    UPDATE contracts
    SET
      last_name_fetch_at = NOW(),
      name = ${name}
    WHERE address = ${address}
  `.then(() => undefined);

const getContractIdentificationStatus = (
  address: string,
): Promise<{ hasName: boolean; lastNameFetchAt: Date | undefined }> =>
  sql`
    SELECT name, last_name_fetch_at FROM contracts
    WHERE address = ${address}
  `.then((rows) => ({
    hasName: typeof rows[0]?.name === "string",
    lastNameFetchAt: rows[0]?.lastNameFetchAt,
  }));

const identifyContract = async (address: string): Promise<void> => {
  const { hasName, lastNameFetchAt } = await getContractIdentificationStatus(
    address,
  );

  if (hasName) {
    return;
  }

  if (
    lastNameFetchAt !== undefined &&
    differenceInHours(new Date(), lastNameFetchAt) > 3
  ) {
    // Don't attempt to fetch contract names more than once every three hours.
    return;
  }

  const abi = await getAbi(address);

  if (abi === undefined) {
    // No contract source yet.
    return updateContractLastNameFetchToNow(address);
  }

  const contract = new web3.eth.Contract(abi, address);
  const hasNameMethod = contract.methods["name"] !== undefined;

  if (!hasNameMethod) {
    // No name method.
    return updateContractLastNameFetchToNow(address);
  }

  const name = await contract.methods.name().call();
  return updateContractName(address, name);
};

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
    T.chain(() => {
      return () =>
        identifyContractQueue.addAll(
          addresses.map((address) => () => identifyContract(address)),
        );
    }),
    T.map(() => undefined),
  );
};

export const getAbi = async (address: string) => {
  return fetch(
    `https://api.etherscan.io/api?module=contract&action=getabi&address=${address}&apikey=${Config.ETHERSCAN_TOKEN}`,
  )
    .then((res) => res.json() as Promise<{ status: "0" | "1"; result: string }>)
    .then((body) =>
      body.status === "1" ? JSON.parse(body.result) : undefined,
    );
};
