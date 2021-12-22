import QuickLRU from "quick-lru";
import { Contract } from "web3-eth-contract";
import * as Etherscan from "./etherscan.js";
import * as EthNode from "./eth_node.js";
import { O, pipe, TE } from "./fp.js";
import * as Log from "./log.js";

const contractsCache = new QuickLRU<string, Contract>({
  maxSize: 1000,
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

export const getName = async (
  contract: Contract,
): Promise<string | undefined> => {
  const hasNameMethod = contract.methods["name"] !== undefined;

  if (!hasNameMethod) {
    return undefined;
  }

  try {
    const name = await contract.methods.name().call();
    return name;
  } catch (error) {
    Log.error(String(error), error);
    return undefined;
  }
};

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
