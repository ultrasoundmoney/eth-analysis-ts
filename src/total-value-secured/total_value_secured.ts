import * as DateFns from "date-fns";
import * as CoinGecko from "../coingecko.js";
import * as Contracts from "../contracts/web3.js";
import * as Db from "../db.js";
import * as Duration from "../duration.js";
import * as EthPrices from "../eth-prices/eth_prices.js";
import * as FamService from "../fam_service.js";
import {
  A,
  B,
  flow,
  MapF,
  NEA,
  O,
  Ord,
  pipe,
  RA,
  RNEA,
  S,
  T,
  TE,
  TO,
} from "../fp.js";
import * as Glassnode from "../glassnode.js";
import * as Log from "../log.js";
import { getStoredMarketCaps } from "../market-caps/market_caps.js";
import * as NftGo from "../nft_go.js";
import * as NftStatic from "./nft_static.js";

export const totalValueSecuredCacheKey = "total-value-secured";

type TotalValueSecured = {
  erc20Leaderboard: TvsRanking[];
  nftLeaderboard: TvsRanking[];
  erc20Total: number;
  nftTotal: number;
  ethTotal: number;
  sum: number;
  securityRatio: number;
};

const storeTotalValueSecured = (totalValueSecured: TotalValueSecured) =>
  pipe(
    Db.sqlTVoid`
      INSERT INTO key_value_store
        ${Db.values({
          key: totalValueSecuredCacheKey,
          value: JSON.stringify(totalValueSecured),
        })}
      ON CONFLICT (key) DO UPDATE SET
        value = excluded.value
      `,
  );

export const getCachedTotalValueSecured = () =>
  pipe(
    Db.sqlT<{ value: TotalValueSecured }[]>`
      SELECT value FROM key_value_store
      WHERE key = ${totalValueSecuredCacheKey}
    `,
    T.map(O.fromNullableK((rows) => rows[0]?.value)),
  );

/**
 * CoinGecko Coin Id
 */
export type CoinId = string;
/**
 * Maps a coingecko ID to an ethereum contract
 */
export type CoinIdContractMap = Map<CoinId, string>;
export type CoinsMaps = {
  onEthOnly: CoinIdContractMap;
  onEthAndOthers: CoinIdContractMap;
};

export type CoinOnEth = {
  id: string;
  symbol: string;
  name: string;
  platforms: Record<"ethereum", string> & Partial<Record<string, string>>;
};

export const getIsCoinOnEth = (coin: CoinGecko.IndexCoin): coin is CoinOnEth =>
  typeof coin.platforms?.ethereum === "string" &&
  coin.platforms.ethereum.length > 0;

export type CoinOnlyOnEth = {
  id: string;
  symbol: string;
  name: string;
  platforms: { ethereum: string };
};

export const getIsCoinOnlyOnEth = (
  coin: CoinGecko.IndexCoin,
): coin is CoinOnlyOnEth =>
  Object.keys(coin.platforms).length === 1 &&
  typeof coin.platforms?.ethereum === "string" &&
  coin.platforms.ethereum.length > 0;

export const getCoinMaps = () =>
  pipe(
    TE.Do,
    TE.apS("coinsList", CoinGecko.getCoinList()),
    TE.bindW(
      "onEthOnly",
      flow(
        ({ coinsList }) => coinsList,
        A.filter(getIsCoinOnlyOnEth),
        A.map((coin) => [coin.id, coin.platforms.ethereum] as [string, string]),
        (entries) => new Map(entries),
        TE.of,
      ),
    ),
    TE.bindW(
      "onEthAndOthers",
      flow(
        ({ coinsList }) => coinsList,
        A.filter(getIsCoinOnEth),
        A.filter((coin) => !getIsCoinOnlyOnEth(coin)),
        A.map((coin) => [coin.id, coin.platforms.ethereum] as [string, string]),
        (entries) => new Map(entries),
        TE.of,
      ),
    ),
    TE.map(({ onEthOnly, onEthAndOthers }) => {
      return { onEthOnly, onEthAndOthers };
    }),
  );
const getSupplyForOnlyOnEthCoins = (
  topCoinMarkets: CoinGecko.CoinMarket[],
  coinsOnlyOnEth: CoinIdContractMap,
) =>
  pipe(
    topCoinMarkets,
    A.map((topCoinMarket) =>
      pipe(
        coinsOnlyOnEth,
        MapF.lookup(S.Eq)(topCoinMarket.id),
        O.map(
          (contractAddress) =>
            [
              topCoinMarket.id,
              {
                circulatingSupply: topCoinMarket.circulating_supply,
                contractAddress,
              },
            ] as [string, CoinSupply],
        ),
      ),
    ),
    A.compact,
    (entries) => new Map(entries),
  );

// Some proxy contracts expose an 'implementation' method that returns the address of the implementation that we can then call. Some do not, and only report their implementation address to Etherscan in an API call. Some we encountered here we've looked up there by hand.
const proxyImplementationMap = new Map([
  // usdc
  [
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
    "0xa2327a938febf5fec13bacfb16ae10ecbc4cbdcf",
  ],
  // busd
  // [
  //   "0x4fabb145d64652a948d72533023f6e7a623c7c53",
  //   "0x5864c777697bf9881220328bf2f16908c9afcd7e",
  // ],
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
]);

type CoinSupply = {
  circulatingSupply: number;
  contractAddress: string;
};

const getSupplyForOnEthAndOthersCoins = (
  coinMarkets: CoinGecko.CoinMarket[],
  coinsOnEthAndOthers: CoinIdContractMap,
) =>
  pipe(
    coinMarkets,
    // A.takeLeft(10),
    // A.filter((coin) => coin.id === "tether"),
    A.map((coinMarket) =>
      pipe(
        coinsOnEthAndOthers,
        MapF.lookup(S.Eq)(coinMarket.id),
        O.map(
          (contractAddress) =>
            [contractAddress, coinMarket] as [string, CoinGecko.CoinMarket],
        ),
      ),
    ),
    A.compact,
    A.map(([contractAddress, coinMarket]) =>
      proxyImplementationMap.has(contractAddress)
        ? ([proxyImplementationMap.get(contractAddress)!, coinMarket] as [
            string,
            CoinGecko.CoinMarket,
          ])
        : ([contractAddress, coinMarket] as [string, CoinGecko.CoinMarket]),
    ),
    T.traverseSeqArray(([contractAddress, coinMarket]) =>
      pipe(
        Contracts.getTotalSupply(contractAddress),
        TE.map((onChainTotalSupply) =>
          // When a coin is present on multiple chains, CoinGecko's circulating supply is across all chains. We need Ethereum only. We take what CoinGecko says to be all tokens out of circulation across all chains (supply - circulating supply), and remove this from what the coin's contract on Ethereum says the total supply on Ethereum is.
          // Contracts appear a little unpredictable, sometimes the supply comes back large and negative, we protect the sum of all coins by capping the lowerbound at zero.
          // In some cases much of the on-chain supply is no longer available. e.g. CRO burned 70B of their supply. When CoinGecko's total supply is lower than on-chain, we use their circulating supply directly.
          coinMarket.total_supply < onChainTotalSupply
            ? coinMarket.circulating_supply
            : Math.max(
                onChainTotalSupply -
                  (coinMarket.total_supply - coinMarket.circulating_supply),
                0,
              ),
        ),
        TE.map((circulatingSupply) => ({
          circulatingSupply,
          contractAddress,
        })),
        // We won't successfully retrieve an on chain total supply for all coins. We log what happened to the rest..
        TE.match(
          (e) => {
            if (e instanceof Contracts.UnsupportedContractError) {
              Log.warn(
                `coin ${coinMarket.symbol}'s contract ${contractAddress} does not support the 'totalSupply' method, skipping!`,
              );
              return O.none;
            }

            Log.error(
              `failed to get on-chain total supply for ${coinMarket.symbol} - ${contractAddress}`,
              e,
            );
            return O.none;
          },
          (coinSupply) =>
            O.some([coinMarket.id, coinSupply] as [string, CoinSupply]),
        ),
      ),
    ),
    T.map(flow(RA.compact, (entries) => new Map(entries))),
  );

type CoinSupplyMap = Map<CoinId, CoinSupply>;
type CoinSupplyMapCache = {
  timestamp: Date;
  erc20Supplies: CoinSupplyMap;
};
let coinSupplyMapCache: O.Option<CoinSupplyMapCache> = O.none;

const getFreshErc20Supplies = () =>
  pipe(
    TE.Do,
    TE.apS("coinMaps", getCoinMaps()),
    TE.apS("coinMarkets", CoinGecko.getTopCoinMarkets()),
    TE.bindW("onEthOnlySupplyMap", ({ coinMarkets, coinMaps }) =>
      pipe(getSupplyForOnlyOnEthCoins(coinMarkets, coinMaps.onEthOnly), TE.of),
    ),
    TE.bind("onEthAndOthersSupplyMap", ({ coinMarkets, coinMaps }) =>
      pipe(
        getSupplyForOnEthAndOthersCoins(coinMarkets, coinMaps.onEthAndOthers),
        (task) => TE.fromTask(task),
      ),
    ),
    TE.map(
      ({ onEthOnlySupplyMap, onEthAndOthersSupplyMap }) =>
        new Map([...onEthOnlySupplyMap, ...onEthAndOthersSupplyMap]),
    ),
    TE.chainFirstIOK((freshErc20Supplies) => () => {
      coinSupplyMapCache = O.some({
        timestamp: new Date(),
        erc20Supplies: freshErc20Supplies,
      });
    }),
  );

const getIsErc20SuppliesCacheFresh = (coinSupplyMapCache: CoinSupplyMapCache) =>
  DateFns.differenceInMilliseconds(new Date(), coinSupplyMapCache.timestamp) <
  Duration.millisFromHours(12);

const getCoinSupplyMap = () =>
  pipe(
    coinSupplyMapCache,
    O.match(
      () => getFreshErc20Supplies(),
      (erc20SuppliesCache) =>
        pipe(
          getIsErc20SuppliesCacheFresh(erc20SuppliesCache),
          B.match(
            () => getFreshErc20Supplies(),
            () => TE.of(erc20SuppliesCache.erc20Supplies),
          ),
        ),
    ),
  );

type CoinV1 = {
  circulatingSupplyAll: number;
  circulatingSupplyEth: number;
  coinGeckoImageUrl: string | null;
  coinGeckoTwitterHandle: string | null;
  coinGeckoUrl: string;
  contractAddress: string;
  id: string;
  marketCapEth: number;
  name: string;
  symbol: string;
  totalSupplyAll: number;
};

const coinV1ByMarketCapDesc: Ord.Ord<CoinV1> = Ord.fromCompare(
  (first, second) =>
    first.marketCapEth > second.marketCapEth
      ? -1
      : first.marketCapEth < second.marketCapEth
      ? 1
      : 0,
);

/**
 * These are the details we have about a coin from our contracts table.
 */
type CoinContractDetails = {
  contractAddress: string;
  name: string | null;
  imageUrl: string | null;
  twitterHandle: string | null;
};

type CoinContractDetailsMap = Map<string, CoinContractDetails>;

const coinV2FromCoinAndDetails = (
  coinContractDetailsMap: Map<string, CoinContractDetails>,
  twitterDetailsMap: Map<string, FamService.TwitterDetails>,
  coin: CoinV1,
): TvsRanking => {
  const contractDetails = pipe(
    coinContractDetailsMap.get(coin.contractAddress),
    O.fromNullable,
  );
  const twitterDetails = pipe(
    contractDetails,
    O.chain(
      O.fromNullableK((contractDetails) => contractDetails.twitterHandle),
    ),
    O.chain(
      O.fromNullableK((twitterHandle) => twitterDetailsMap.get(twitterHandle)),
    ),
  );
  if (coin.id === "radio-caca") {
    console.log(
      coin,
      contractDetails,
      twitterDetails,
      pipe(
        twitterDetails,
        O.map((details) => details.followerCount),
        O.toUndefined,
      ),
    );
  }
  return {
    famFollowerCount: pipe(
      twitterDetails,
      O.chain(O.fromNullableK((details) => details.famFollowerCount)),
      O.toUndefined,
    ),
    followerCount: pipe(
      twitterDetails,
      O.map((details) => details.followerCount),
      O.toUndefined,
    ),
    imageUrl: pipe(
      contractDetails,
      O.chain(O.fromNullableK((details) => details.imageUrl)),
      O.alt(O.fromNullableK(() => coin.coinGeckoImageUrl)),
      O.toUndefined,
    ),
    imageUrlAlt: pipe(coin.coinGeckoImageUrl, O.fromNullable, O.toUndefined),
    links: pipe(
      twitterDetails,
      O.chain(O.fromNullableK((details) => details.links)),
      O.toUndefined,
    ),
    marketCap: coin.marketCapEth,
    name: pipe(
      contractDetails,
      O.chain(O.fromNullableK((details) => details.name)),
      O.getOrElse(() => coin.name),
    ),
    detail: coin.symbol,
    tooltipDescription: pipe(
      twitterDetails,
      O.chain(O.fromNullableK((details) => details.bio)),
      O.toUndefined,
    ),
    twitterUrl: pipe(
      contractDetails,
      O.chain(O.fromNullableK((details) => details.twitterHandle)),
      O.alt(O.fromNullableK(() => coin.coinGeckoTwitterHandle)),
      O.map((handle) => `https://twitter.com/${handle}`),
      O.toUndefined,
    ),
    coinGeckoUrl: coin.coinGeckoUrl,
    nftGoUrl: undefined,
    tooltipName: pipe(
      contractDetails,
      O.chain(O.fromNullableK((details) => details.name)),
      O.getOrElse(() => coin.name),
    ),
  };
};

const getTopErc20s = () =>
  pipe(
    TE.Do,
    TE.apS("coinSupplyMap", getCoinSupplyMap()),
    TE.apSW("coinMarkets", CoinGecko.getTopCoinMarkets()),
    TE.map(({ coinSupplyMap, coinMarkets }) =>
      pipe(
        coinMarkets,
        A.map((coinMarket) =>
          pipe(
            coinSupplyMap,
            MapF.lookup(S.Eq)(coinMarket.id),
            O.map(
              (coinSupply) =>
                [coinSupply, coinMarket] as [CoinSupply, CoinGecko.CoinMarket],
            ),
          ),
        ),
        A.partition(O.isSome),
        ({ left, right }) =>
          pipe(
            Log.debug(
              `out of top 250 coins, ${left.length} were not on ETH or are on ETH but we haven't fetched their circulating supply yet`,
            ),
            () => A.compact(right),
          ),
        A.map(
          ([coinSupply, coinMarket]): CoinV1 => ({
            circulatingSupplyAll: coinMarket.circulating_supply,
            circulatingSupplyEth: coinSupply.circulatingSupply,
            coinGeckoImageUrl: coinMarket.image,
            coinGeckoUrl: `https://www.coingecko.com/en/coins/${coinMarket.id}`,
            contractAddress: coinSupply.contractAddress,
            id: coinMarket.id,
            marketCapEth:
              coinSupply.circulatingSupply * coinMarket.current_price,
            symbol: coinMarket.symbol,
            totalSupplyAll: coinMarket.total_supply,
            name: coinMarket.name,
            coinGeckoTwitterHandle: coinMarket.twitter_handle,
          }),
        ),
        A.map((coin) => {
          if (coin.marketCapEth < 1e6) {
            Log.warn(
              `coin ${coin.symbol} market cap suspiciously low: ${coin.marketCapEth}, contract: ${coin.contractAddress}`,
            );
          }
          return coin;
        }),
      ),
    ),
  );

const getErc20MarketCapSum = (erc20MarketCaps: CoinV1[]) =>
  pipe(
    erc20MarketCaps,
    A.reduce(0, (sum, coin) => sum + coin.marketCapEth),
  );

const getEthMarketCap = () =>
  pipe(
    getStoredMarketCaps(),
    TO.map((storedMarketCaps) => storedMarketCaps.ethMarketCap),
  );

const getSecurityRatio = (
  erc20Total: number,
  nftTotal: number,
  ethMarketCap: number,
) =>
  pipe(
    TE.Do,
    TE.apS("ethStaked", Glassnode.getEthStaked()),
    TE.apSW("ethPrice", EthPrices.getEthPrice(new Date())),
    TE.map(
      ({ ethStaked, ethPrice }) =>
        (erc20Total + nftTotal + ethMarketCap) / (ethStaked * ethPrice.ethusd),
    ),
  );

type TwitterHandle = string;

const getDbDetailsForCoins = flow(
  A.map((coin: CoinV1) => coin.contractAddress),
  (addresses) => Db.sqlT<CoinContractDetails[]>`
      SELECT
        address as contract_address,
        image_url,
        name,
        twitter_handle
      FROM contracts
      WHERE address IN (${addresses})
    `,
  T.map(
    flow(
      A.map(
        (details) =>
          [details.contractAddress, details] as [string, CoinContractDetails],
      ),
      (entries) => new Map(entries),
    ),
  ),
);

const getTwitterDetailsForCoins = (
  contractDetailsMap: CoinContractDetailsMap,
) =>
  pipe(
    TE.Do,
    TE.apS(
      "coinTwitterHandles",
      pipe(
        Array.from(contractDetailsMap.values()),
        A.map(
          flow(
            O.fromNullableK((details) =>
              typeof details.twitterHandle === "string" &&
              details.twitterHandle.length !== 0
                ? details.twitterHandle
                : null,
            ),
          ),
        ),
        A.compact,
        TE.of,
      ),
    ),
    TE.bindW("twitterDetails", ({ coinTwitterHandles }) =>
      pipe(
        coinTwitterHandles,
        NEA.fromArray,
        O.match(
          () => TE.of([]),
          (handles) => FamService.getDetailsByHandles(handles),
        ),
      ),
    ),
    TE.map(({ twitterDetails }) =>
      pipe(
        twitterDetails,
        A.map(
          (details) =>
            [details.handle, details] as [
              TwitterHandle,
              FamService.TwitterDetails,
            ],
        ),
        (entries) => new Map(entries),
      ),
    ),
  );

const getTwitterDetailsForCollections = (collections: NftGo.Collection[]) =>
  pipe(
    collections,
    A.map(
      flow(
        O.fromNullableK((collection) => collection.medias.twitter),
        O.map(flow(S.split("/"), RNEA.last)),
      ),
    ),
    A.compact,
    NEA.fromArray,
    O.match(
      () => TE.of([]),
      (handles) => FamService.getDetailsByHandles(handles),
    ),
    TE.map((twitterDetails) =>
      pipe(
        twitterDetails,
        A.map(
          (details) =>
            [details.handle, details] as [
              TwitterHandle,
              FamService.TwitterDetails,
            ],
        ),
        (entries) => new Map(entries),
      ),
    ),
  );

type TvsRanking = {
  coinGeckoUrl: string | undefined;
  detail: string | undefined;
  famFollowerCount: number | undefined;
  followerCount: number | undefined;
  imageUrl: string | undefined;
  imageUrlAlt: string | undefined;
  links: FamService.Linkable[] | undefined;
  marketCap: number;
  name: string;
  nftGoUrl: string | undefined;
  tooltipDescription: string | undefined;
  tooltipName: string | undefined;
  twitterUrl: string | undefined;
};

const getErc20Leaderboard = (topErc20s: CoinV1[]) =>
  pipe(
    TE.Do,
    TE.bind("contractDetailsMap", () =>
      TE.fromTask(getDbDetailsForCoins(topErc20s)),
    ),
    TE.bindW("twitterDetailsMap", ({ contractDetailsMap }) =>
      getTwitterDetailsForCoins(contractDetailsMap),
    ),
    TE.map(({ contractDetailsMap, twitterDetailsMap }) =>
      pipe(
        topErc20s,
        A.sort(coinV1ByMarketCapDesc),
        A.map(
          (coin): TvsRanking =>
            coinV2FromCoinAndDetails(
              contractDetailsMap,
              twitterDetailsMap,
              coin,
            ),
        ),
      ),
    ),
  );

const tvsRankingFromNftCollection = (
  twitterDetailsMap: Map<string, FamService.TwitterDetails>,
  collection: NftGo.Collection,
): TvsRanking =>
  pipe(
    collection.medias.twitter,
    O.fromNullable,
    O.map(flow(S.split("/"), RNEA.last)),
    O.chainNullableK((handle) => twitterDetailsMap.get(handle)),
    O.matchW(
      () => ({
        coinGeckoUrl: undefined,
        detail: undefined,
        famFollowerCount: undefined,
        followerCount: undefined,
        imageUrl: collection.logo,
        imageUrlAlt: undefined,
        links: undefined,
        marketCap: collection.marketCap,
        name: collection.name,
        nftGoUrl: `https://nftgo.io/collection/${collection.slug}/`,
        tooltipDescription: collection.longDesc,
        tooltipName: collection.name,
        twitterUrl: undefined,
      }),
      (twitterDetails) => ({
        coinGeckoUrl: undefined,
        detail: undefined,
        famFollowerCount: twitterDetails.famFollowerCount,
        followerCount: twitterDetails.followerCount,
        imageUrl: collection.logo,
        imageUrlAlt: undefined,
        links: twitterDetails.links ?? undefined,
        marketCap: collection.marketCap,
        name: collection.name,
        nftGoUrl: `https://nftgo.io/collection/${collection.slug}/`,
        tooltipDescription: collection.longDesc,
        tooltipName: collection.name,
        twitterUrl: `https://twitter.com/${twitterDetails.handle}`,
      }),
    ),
  );

const getNftLeaderboard = () =>
  pipe(
    TE.Do,
    TE.apS(
      "rankedCollections",
      pipe(
        NftGo.getRankedCollections(),
        TE.altW(() => TE.of(NftStatic.rankedCollections)),
      ),
    ),
    TE.apS(
      "twitterDetailsMap",
      getTwitterDetailsForCollections(NftStatic.rankedCollections),
    ),
    TE.map(({ rankedCollections, twitterDetailsMap }) =>
      pipe(
        rankedCollections,
        A.filter((collection) => collection.blockchain === "ETH"),
        A.takeLeft(20),
        A.map((collection) =>
          tvsRankingFromNftCollection(twitterDetailsMap, collection),
        ),
      ),
    ),
  );

const getTotalValueSecured = () =>
  pipe(
    TE.Do,
    TE.apS("erc20Coins", getTopErc20s()),
    TE.apSW("nftLeaderboard", getNftLeaderboard()),
    TE.apSW(
      "nftTotal",
      pipe(
        NftGo.getMarketCap(),
        TE.altW(() => TE.of(NftStatic.nftMarketCap)),
      ),
    ),
    TE.apSW(
      "ethMarketCap",
      pipe(
        getEthMarketCap(),
        TE.fromTaskOption(
          () => new Error("failed to build summary, no eth market cap stored"),
        ),
      ),
    ),
    TE.bindW("erc20Leaderboard", ({ erc20Coins }) =>
      getErc20Leaderboard(erc20Coins),
    ),
    TE.bindW("erc20Total", ({ erc20Coins }) =>
      pipe(erc20Coins, getErc20MarketCapSum, TE.of),
    ),
    TE.bindW("securityRatio", ({ erc20Total, nftTotal, ethMarketCap }) =>
      getSecurityRatio(erc20Total, nftTotal, ethMarketCap),
    ),
    TE.map(
      ({
        erc20Leaderboard,
        erc20Total,
        ethMarketCap,
        nftLeaderboard,
        nftTotal,
        securityRatio,
      }) => ({
        erc20Leaderboard,
        erc20Total,
        ethTotal: ethMarketCap,
        nftLeaderboard,
        nftTotal,
        securityRatio,
        sum: erc20Total + nftTotal + ethMarketCap,
      }),
    ),
  );

export const updateTotalValueSecured = () =>
  pipe(
    getTotalValueSecured(),
    TE.chainTaskK((totalValueSecured) =>
      storeTotalValueSecured(totalValueSecured),
    ),
    TE.chainTaskK(() =>
      Db.sqlTNotify("cache-update", totalValueSecuredCacheKey),
    ),
  );
