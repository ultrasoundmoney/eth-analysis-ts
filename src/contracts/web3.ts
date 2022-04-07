import QuickLRU from "quick-lru";
import { Contract } from "web3-eth-contract";
import * as Etherscan from "../etherscan.js";
import * as EthNode from "../eth_node.js";
import * as FetchAlt from "../fetch_alt.js";
import { B, O, OAlt, pipe, TE, TEAlt } from "../fp.js";
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
export class ZeroSupplyError extends Error {}

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

const getErc20TotalSupply = (contract: Contract) =>
  pipe(
    getIsErc20Contract(contract),
    B.match(
      () => TE.left(new UnsupportedContractError()),
      () =>
        getIsErc20ContractWithDecimals(contract)
          ? getErc20TotalSupplyWithDecimals(contract)
          : getErc20TotalSupplyPlain(contract),
    ),
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

const getErc20ProxyTotalSupply = (proxyContract: ProxyContract) =>
  pipe(
    getIsProxyContract(proxyContract),
    B.match(
      () =>
        TE.left(
          new UnsupportedContractError(
            `contract ${proxyContract.options.address} is not a proxy contract`,
          ),
        ),
      () =>
        pipe(
          TE.tryCatch(
            () => proxyContract.methods.implementation().call(),
            decodeContractCallError,
          ),
          TE.chainW((implementationAddress) =>
            getContract(implementationAddress),
          ),
          TE.chainW((implementationContract) =>
            getIsErc20Contract(implementationContract)
              ? getErc20TotalSupply(implementationContract)
              : TE.left(
                  new UnsupportedContractError(
                    `expected ERC20 proxy implementation contract ${implementationContract.options.address} to have an totalSupply method but it didn't`,
                  ),
                ),
          ),
          // Unstructured proxies should not be passed to this method, if it does happen the contract might return a bad value of a correct type. We try to protect against this a little.
          TE.chainW((totalSupply) =>
            totalSupply === 0
              ? TE.left(
                  new ZeroSupplyError(
                    `proxy contract ${proxyContract.options.address} total supply came back 0, and is probably wrong`,
                  ),
                )
              : TE.right(totalSupply),
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
        ),
    ),
  );

// Some proxy contracts expose an 'implementation' method that returns the address of the implementation that we can then call. Some do not, and only report their implementation address to Etherscan in an API call. Some we encountered here we've looked up there by hand.
// https://docs.openzeppelin.com/upgrades-plugins/1.x/proxies
type ProxyAddress = string;
type ImplementationAddress = string;
const unstructuredProxyMap: Map<ProxyAddress, ImplementationAddress> = new Map([
  // usdc
  [
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
    "0xa2327a938febf5fec13bacfb16ae10ecbc4cbdcf",
  ],
  // busd
  [
    "0x4fabb145d64652a948d72533023f6e7a623c7c53",
    "0x5864c777697bf9881220328bf2f16908c9afcd7e",
  ],
  // aave
  [
    "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9",
    "0xc13eac3b4f9eed480045113b7af00f7b5655ece8",
  ],
  // rndr
  [
    "0x6de037ef9ad2725eb40118bb1702ebb27e4aeb24",
    "0x1a1fdf27c5e6784d1cebf256a8a5cc0877e73af0",
  ],
  // paxg
  [
    "0x45804880de22913dafe09f4980848ece6ecbaf78",
    "0x74271f2282ed7ee35c166122a60c9830354be42a",
  ],
  // renbtc
  [
    "0xeb4c2781e4eba804ce9a9803c67d0893436bb27d",
    "0xe2d6ccac3ee3a21abf7bedbe2e107ffc0c037e80",
  ],
  // knc
  [
    "0xdefa4e8a7bcba345f687a2f1456f5edd9ce97202",
    "0xe5e8e834086f1a964f9a089eb6ae11796862e4ce",
  ],
  // tusd
  [
    "0x0000000000085d4780b73119b644ae5ecd22b376",
    "0xd8d59c59ab40b880b54c969920e8d9172182ad7b",
  ],
  // okb
  [
    "0x75231f58b43240c9718dd58b4967c5114342a86c",
    "0x5dba7dfcdbfb8812d30fdd99d9441f8b7a605621",
  ],
]);

const getIsErc20UnstructuredProxy = (contract: Contract) =>
  unstructuredProxyMap.has(contract.options.address.toLowerCase());

const getErc20UnstructuredProxyTotalSupply = (contract: Contract) =>
  pipe(
    unstructuredProxyMap.get(contract.options.address.toLowerCase()),
    O.fromNullable,
    OAlt.getOrThrow("expected contract to be present in unstructuredProxy map"),
    (implementationAddress) => Etherscan.getAbi(implementationAddress),
    // To call unstructured proxies we create a contract with the proxy address, but the implementation ABI.
    TE.map((abi) =>
      EthNode.makeContract(contract.options.address.toLowerCase(), abi),
    ),
    TE.chainW(getErc20TotalSupply),
  );

export const getTotalSupply = (address: string) =>
  pipe(
    getContract(address),
    TE.chainW((contract) =>
      pipe(
        getIsErc20UnstructuredProxy(contract),
        B.match(
          () =>
            pipe(
              getErc20TotalSupply(contract),
              TE.alt(() => getErc20ProxyTotalSupply(contract)),
            ),
          // When the contract is an unstructured proxy, _don't_ call it as a normal proxy. The contract will answer fine but have no access to its storage, returning wrong values of the correct type.
          () => getErc20UnstructuredProxyTotalSupply(contract),
        ),
      ),
    ),
    TE.mapLeft(
      (
        e,
      ):
        | FetchAlt.FetchError
        | Error
        | FetchAlt.BadResponseError
        | FetchAlt.DecodeJsonError
        | UnsupportedContractError => {
        if (e instanceof Etherscan.AbiNotVerifiedError) {
          return new UnsupportedContractError(e.message);
        }
        return e;
      },
    ),
  );
