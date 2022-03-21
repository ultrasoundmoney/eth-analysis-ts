import QuickLRU from "quick-lru";
import { Contract } from "web3-eth-contract";
import * as Etherscan from "../etherscan.js";
import * as EthNode from "../eth_node.js";
import { B, flow, O, pipe, TE, TEAlt } from "../fp.js";
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

          return TEAlt.decodeUnknownError(e);
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
            TEAlt.decodeUnknownError,
          ),
        ),
      () => TE.left(new UnsupportedMethodError()),
    ),
  );

type Erc20Contract = Omit<Contract, "methods"> & {
  methods: { totalSupply: () => { call: () => Promise<number> } };
};

type ProxyContract = Omit<Contract, "methods"> & {
  methods: { implementation: () => { call: () => Promise<string> } };
};

const getIsProxyContract = (contract: Contract) =>
  contract.methods.implementation !== undefined;

const getIsErc20Contract = (contract: Contract) =>
  contract.methods.totalSupply !== undefined;

const getErc20TotalSupply = TE.tryCatchK(
  (erc20Contract: Erc20Contract) => erc20Contract.methods.totalSupply().call(),
  TEAlt.decodeUnknownError,
);

export class UnsupportedContractError extends Error {}
export class ExecutionRevertedError extends Error {}

const decodeContractCallError = (e: unknown) => {
  if (e instanceof Error) {
    if (e.message.includes("execution reverted")) {
      return new ExecutionRevertedError();
    }

    return e;
  }

  return new Error(String(e));
};

const getErc20ProxyTotalSupply = flow(
  TE.tryCatchK(
    (proxyContract: ProxyContract) =>
      proxyContract.methods.implementation().call(),
    decodeContractCallError,
  ),
  TE.chainW((implementationAddress) => getContract(implementationAddress)),
  TE.chainW((implementationContract) =>
    pipe(
      getIsErc20Contract(implementationContract),
      B.match(
        () =>
          TE.left(
            new UnsupportedContractError(
              `expected ERC20 proxy implementation contract ${implementationContract.options.address} to have an totalSupply method but it didn't`,
            ),
          ),
        () => getErc20TotalSupply(implementationContract),
      ),
    ),
  ),
  TE.mapLeft(
    (e): UnsupportedContractError | Etherscan.FetchAbiError | Error => {
      // We expect for this to happen on some proxy contracts that make their implementation method adminOnly, consider them simply unsupported.
      if (e instanceof ExecutionRevertedError) {
        return new UnsupportedContractError(e.message);
      }

      return e;
    },
  ),
);

export const getTotalSupply = (address: string) =>
  pipe(
    getContract(address),
    TE.chainW((contract) =>
      getIsErc20Contract(contract)
        ? getErc20TotalSupply(contract)
        : getIsProxyContract(contract)
        ? getErc20ProxyTotalSupply(contract)
        : TE.left(new UnsupportedContractError()),
    ),
  );
