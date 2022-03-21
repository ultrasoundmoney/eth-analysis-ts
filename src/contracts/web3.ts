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

interface Erc20Contract extends Contract {
  methods: {
    totalSupply: () => {
      call: () => Promise<number>;
    };
  };
}

const getIsErc20Contract = (contract: Contract): contract is Erc20Contract =>
  contract.methods.totalSupply !== undefined;

interface Erc20ContractWithDecimals extends Contract {
  methods: {
    decimals: () => { call: () => Promise<number> };
    totalSupply: () => { call: () => Promise<number> };
  };
}

const getIsErc20ContractWithDecimals = (
  contract: Contract,
): contract is Erc20ContractWithDecimals =>
  contract.methods.totalSupply !== undefined &&
  contract.methods.decimals !== undefined;

interface ProxyContract extends Contract {
  methods: { implementation: () => { call: () => Promise<string> } };
}

const getIsProxyContract = (contract: Contract): contract is ProxyContract =>
  contract.methods.implementation !== undefined;

const getErc20TotalSupplyPlain = TE.tryCatchK(
  (erc20Contract: Erc20Contract) => erc20Contract.methods.totalSupply().call(),
  TEAlt.decodeUnknownError,
);

const getErc20TotalSupplyWithDecimals = (contract: Erc20ContractWithDecimals) =>
  pipe(
    TE.Do,
    TE.apS(
      "decimals",
      TE.tryCatch(
        () => contract.methods.decimals().call(),
        TEAlt.decodeUnknownError,
      ),
    ),
    TE.apS(
      "totalSupply",
      TE.tryCatch(
        () => contract.methods.totalSupply().call(),
        TEAlt.decodeUnknownError,
      ),
    ),
    TE.map(({ decimals, totalSupply }) => totalSupply / 10 ** decimals),
  );

const getErc20TotalSupply = (contract: Erc20Contract) =>
  pipe(
    getIsErc20ContractWithDecimals(contract)
      ? getErc20TotalSupplyWithDecimals(contract)
      : getErc20TotalSupplyPlain(contract),
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
    getIsErc20Contract(implementationContract)
      ? getErc20TotalSupply(implementationContract)
      : TE.left(
          new UnsupportedContractError(
            `expected ERC20 proxy implementation contract ${implementationContract.options.address} to have an totalSupply method but it didn't`,
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
