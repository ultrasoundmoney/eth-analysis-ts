import QuickLRU from "quick-lru";
import { Contract } from "web3-eth-contract";
import * as Etherscan from "../etherscan.js";
import * as EthNode from "../eth_node.js";
import { O, pipe, TE, TEAlt } from "../fp.js";
import * as Log from "../log.js";

// NOTE: We already cache ABIs and creating contracts is cheap, but it turns out web3js leaks memory when creating new contracts. There's a _years_ old issue describing the problem here: https://github.com/ChainSafe/web3.js/issues/3042 . We'd like to switch to ethers-js for this and various other reasons. Until then, we cache contracts to alleviate the problem a little.
const contractsCache = new QuickLRU<string, Contract>({
  maxSize: 2000,
});

const getCachedContract = (address: string) =>
  pipe(contractsCache.get(address), O.fromNullable);

const fetchAndCacheContract = (address: string) =>
  pipe(
    Etherscan.getAbi(address),
    TE.map((abi) => EthNode.makeContract(address, abi)),
    TE.chainFirstIOK((contract) => () => {
      contractsCache.set(address, contract);
    }),
  );

export const getContract = (
  address: string,
): TE.TaskEither<Etherscan.FetchAbiError, Contract> =>
  pipe(
    getCachedContract(address),
    O.match(() => fetchAndCacheContract(address), TE.right),
  );

export class NoNameMethodError extends Error {}

export const getName = (
  contract: Contract,
): TE.TaskEither<NoNameMethodError | Error, string> =>
  contract.methods["name"] !== undefined
    ? TE.tryCatch(
        () => contract.methods.name().call(),
        (e) => {
          Log.error(
            `name method present but call failed for ${contract.options.address}`,
          );
          return TEAlt.errorFromUnknown(e);
        },
      )
    : TE.left(new NoNameMethodError());

type InterfaceId = "ERC721" | "ERC1155";

const interfaceSignatureMap: Record<InterfaceId, string> = {
  ERC721: "0x80ac58cd",
  ERC1155: "0xd9b67a26",
};

export const getSupportedInterface = async (
  contract: Contract,
  interfaceId: InterfaceId,
): Promise<boolean | undefined> => {
  const hasSupportedInterfaceMethod =
    contract.methods["supportsInterface"] !== undefined;

  if (!hasSupportedInterfaceMethod) {
    return false;
  }

  const signature = interfaceSignatureMap[interfaceId];

  try {
    const interfaceSupported = await contract.methods
      .supportsInterface(signature)
      .call();
    return interfaceSupported;
  } catch (error) {
    Log.error(String(error), error);
    return undefined;
  }
};
