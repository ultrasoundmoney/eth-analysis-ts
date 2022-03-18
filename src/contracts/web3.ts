import QuickLRU from "quick-lru";
import { Contract } from "web3-eth-contract";
import * as Etherscan from "../etherscan.js";
import * as EthNode from "../eth_node.js";
import { B, O, pipe, TE, TEAlt } from "../fp.js";
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

export const getContract = (address: string) =>
  pipe(
    getCachedContract(address),
    O.match(() => fetchAndCacheContract(address), TE.right),
  );

export class NoNameMethodError extends Error {}
export class NameMethodRequiresParamError extends Error {}

export const getName = (
  contract: Contract,
): TE.TaskEither<NoNameMethodError | Error, string> =>
  contract.methods["name"] !== undefined
    ? TE.tryCatch(
        () => contract.methods.name().call(),
        (e) => {
          if (
            e instanceof Error &&
            e.message.startsWith("Invalid number of parameters for")
          ) {
            Log.debug(
              `name method requires param for contract ${contract.options.address}`,
            );
            return new NameMethodRequiresParamError();
          }

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

export class UnsupportedMethodError extends Error {}

export const getSupportedInterface = (
  contract: Contract,
  interfaceId: InterfaceId,
): TE.TaskEither<UnsupportedMethodError | Error, boolean> =>
  pipe(
    contract.methods["supportsInterface"] === undefined,
    B.match(
      () =>
        pipe(interfaceSignatureMap[interfaceId], (signature) =>
          TE.tryCatch(
            () =>
              contract.methods
                .supportsInterface(signature)
                .call() as Promise<boolean>,
            TEAlt.errorFromUnknown,
          ),
        ),
      () => TE.left(new UnsupportedMethodError()),
    ),
  );

export const getTotalSupply = (contract: Contract) =>
  pipe(
    contract.methods["totalSupply"] === undefined,
    B.match(
      () =>
        TE.tryCatch(
          () => contract.methods.totalSupply().call() as Promise<number>,
          TEAlt.errorFromUnknown,
        ),
      () => TE.left(new UnsupportedMethodError()),
    ),
  );
