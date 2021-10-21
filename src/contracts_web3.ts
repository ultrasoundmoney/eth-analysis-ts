import * as Etherscan from "./etherscan.js";
import * as Log from "./log.js";
import { Contract } from "web3-eth-contract";
import { pipe, T } from "./fp.js";
import { web3 } from "./eth_node.js";

export const getWeb3Contract = (
  address: string,
): T.Task<Contract | undefined> =>
  pipe(
    () => Etherscan.getAbiWithCache(address),
    T.map((abi) => {
      if (abi === undefined) {
        return abi;
      }

      if (web3 === undefined) {
        throw new Error("tried to call web3 method before connecting");
      }

      const contract = new web3.eth.Contract(abi, address);

      return contract;
    }),
  );

export const getName = async (
  contract: Contract,
): Promise<string | undefined> => {
  const hasNameMethod = contract.methods["name"] !== undefined;

  if (!hasNameMethod) {
    return undefined;
  }

  return contract.methods.name().call();
};

type InterfaceId = "ERC721" | "ERC1155";

const interfaceSignatureMap: Record<InterfaceId, string> = {
  ERC721: "0x80ac58cd",
  ERC1155: "0xd9b67a26",
};

export const getSupportedInterface = (
  contract: Contract,
  interfaceId: InterfaceId,
): Promise<boolean> => {
  const hasSupportedInterfaceMethod =
    contract.methods["supportsInterface"] !== undefined;

  if (!hasSupportedInterfaceMethod) {
    Log.debug("missing method");
    return Promise.resolve(false);
  }

  const signature = interfaceSignatureMap[interfaceId];

  return (
    contract.methods.supportsInterface(signature).call() ||
    contract.methods.supportsInterface(signature).call()
  );
};
