import * as EthNode from "./eth_node.js";
import * as Etherscan from "./etherscan.js";
import * as Log from "./log.js";
import { Contract } from "web3-eth-contract";
import { pipe, T } from "./fp.js";

export const getWeb3Contract = (
  address: string,
): T.Task<Contract | undefined> =>
  pipe(
    () => Etherscan.getAbiWithCache(address),
    T.map((abi) => {
      if (abi === undefined) {
        return undefined;
      }

      return EthNode.makeContract(address, abi);
    }),
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
