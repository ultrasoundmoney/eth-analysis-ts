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
  OAlt,
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
            if (e instanceof Contracts.ZeroSupplyError) {
              Log.warn(
                `coin ${coinMarket.symbol} returned zero supply, skipping`,
                e,
              );
              return O.none;
            }

            if (e instanceof Contracts.UnsupportedContractError) {
              Log.warn(
                `coin ${coinMarket.symbol}'s is unsupported, skipping`,
                e,
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
  coinGeckoUrl: string;
  contractAddress: string;
  id: string;
  imageUrl: string | null;
  marketCapEth: number;
  name: string;
  symbol: string;
  totalSupplyAll: number;
  twitterHandle: string | undefined;
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
type ContractDetailsRow = {
  contractAddress: string;
  name: string | null;
  imageUrl: string | null;
  twitterHandle: string | null;
};

type ContractDetails = {
  contractAddress: string;
  name: string | undefined;
  imageUrl: string | undefined;
  twitterHandle: string | undefined;
};

type ContractDetailsMap = Map<string, ContractDetails>;
type TwitterDetailsMap = Map<TwitterHandle, FamService.TwitterDetails>;

const buildRankingFromCoin = (coin: CoinV1): TvsRanking => ({
  coinGeckoName: coin.name,
  coinGeckoTwitterHandle: coin.twitterHandle,
  coinGeckoUrl: `https://www.coingecko.com/en/coins/${coin.id}`,
  contractAddresses: NEA.of(coin.contractAddress),
  detail: coin.symbol,
  famFollowerCount: undefined,
  followerCount: undefined,
  imageUrl: undefined,
  links: undefined,
  marketCap: coin.marketCapEth,
  name: undefined,
  nftGoName: undefined,
  nftGoTwitterHandle: undefined,
  nftGoUrl: undefined,
  tooltipDescription: undefined,
  tooltipName: undefined,
  twitterUrl: undefined,
});

const buildRankingWithContractDetailsFromCoin = (
  contractDetails: ContractDetails,
  coin: CoinV1,
): TvsRanking => ({
  ...buildRankingFromCoin(coin),
  contractAddresses: NEA.of(contractDetails.contractAddress),
  name: contractDetails.name,
  imageUrl: contractDetails.imageUrl,
});

const buildRankingWithAllDetailsFromCoin = (
  contractDetails: ContractDetails,
  twitterDetails: FamService.TwitterDetails,
  coin: CoinV1,
): TvsRanking => ({
  ...buildRankingWithContractDetailsFromCoin(contractDetails, coin),
  famFollowerCount: twitterDetails.famFollowerCount,
  followerCount: twitterDetails.followerCount,
  links: twitterDetails.links,
  tooltipDescription: twitterDetails.bio,
  tooltipName: twitterDetails.name,
  twitterUrl: `https://twitter.com/${twitterDetails.handle}`,
});

const coinV2FromCoinAndDetails = (
  coinContractDetailsMap: Map<string, ContractDetails>,
  twitterDetailsMap: TwitterDetailsMap,
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

  return pipe(
    contractDetails,
    O.match(
      () => buildRankingFromCoin(coin),
      (contractDetails) =>
        pipe(
          twitterDetails,
          O.match(
            () =>
              buildRankingWithContractDetailsFromCoin(contractDetails, coin),
            (twitterDetails) =>
              buildRankingWithAllDetailsFromCoin(
                contractDetails,
                twitterDetails,
                coin,
              ),
          ),
        ),
    ),
  );
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
            imageUrl: coinMarket.image,
            coinGeckoUrl: `https://www.coingecko.com/en/coins/${coinMarket.id}`,
            contractAddress: coinSupply.contractAddress,
            id: coinMarket.id,
            marketCapEth:
              coinSupply.circulatingSupply * coinMarket.current_price,
            symbol: coinMarket.symbol,
            totalSupplyAll: coinMarket.total_supply,
            name: coinMarket.name,
            twitterHandle: coinMarket.twitter_handle ?? undefined,
          }),
        ),
        A.filter((coin) => {
          if (coin.marketCapEth < 1e6) {
            Log.warn(
              `coin ${coin.symbol} market cap suspiciously low: ${coin.marketCapEth}, contract: ${coin.contractAddress}, skipping`,
            );
            return false;
          }

          return true;
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

const getContractDetailsForAddresses = (addresses: string[]) =>
  pipe(
    Db.sqlT<ContractDetailsRow[]>`
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
        A.map((details) => ({
          ...details,
          name: details.name ?? undefined,
          imageUrl: details.imageUrl ?? undefined,
          twitterHandle: details.twitterHandle ?? undefined,
        })),
        A.map(
          (details) =>
            [details.contractAddress, details] as [string, ContractDetails],
        ),
        (entries) => new Map(entries),
      ),
    ),
  );

const getTwitterDetailsForCoins = (contractDetailsMap: ContractDetailsMap) =>
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
  coinGeckoName: string | undefined;
  coinGeckoTwitterHandle: string | undefined;
  coinGeckoUrl: string | undefined;
  contractAddresses: NEA.NonEmptyArray<string>;
  detail: string | undefined;
  famFollowerCount: number | undefined;
  followerCount: number | undefined;
  imageUrl: string | undefined;
  links: FamService.Linkables | undefined;
  marketCap: number;
  name: string | undefined;
  nftGoName: string | undefined;
  nftGoTwitterHandle: string | undefined;
  nftGoUrl: string | undefined;
  tooltipDescription: string | undefined;
  tooltipName: string | undefined;
  twitterUrl: string | undefined;
};

const getErc20Leaderboard = (topErc20s: CoinV1[]) =>
  pipe(
    TE.Do,
    TE.bind("contractDetailsMap", () =>
      pipe(
        topErc20s,
        A.map((coin) => coin.contractAddress),
        (addresses) => TE.fromTask(getContractDetailsForAddresses(addresses)),
      ),
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

const buildRankingFromCollection = (
  collection: NftGo.Collection,
): TvsRanking => ({
  coinGeckoName: undefined,
  coinGeckoTwitterHandle: undefined,
  coinGeckoUrl: undefined,
  contractAddresses: pipe(
    collection.contracts,
    NEA.fromArray,
    OAlt.getOrThrow(
      "expected collection to contain at least one contract address",
    ),
  ),
  detail: undefined,
  famFollowerCount: undefined,
  followerCount: undefined,
  imageUrl: undefined,
  links: undefined,
  marketCap: collection.marketCap,
  name: undefined,
  nftGoName: undefined,
  nftGoTwitterHandle: undefined,
  nftGoUrl: undefined,
  tooltipDescription: undefined,
  tooltipName: undefined,
  twitterUrl: undefined,
});

const buildRankingWithContractDetailsFromCollection = (
  contractDetails: ContractDetails,
  collection: NftGo.Collection,
): TvsRanking => ({
  ...buildRankingFromCollection(collection),
  name: contractDetails.name,
  imageUrl: contractDetails.imageUrl,
});

const buildRankingWithAllDetailsFromCollection = (
  contractDetails: ContractDetails,
  twitterDetails: FamService.TwitterDetails,
  collection: NftGo.Collection,
): TvsRanking => ({
  ...buildRankingWithContractDetailsFromCollection(contractDetails, collection),
  famFollowerCount: twitterDetails.famFollowerCount,
  followerCount: twitterDetails.followerCount,
  links: twitterDetails.links,
  tooltipDescription: twitterDetails.bio,
  tooltipName: twitterDetails.name,
  twitterUrl: `https://twitter.com/${twitterDetails.handle}`,
});

const tvsRankingFromNftCollection = (
  contractDetailsMap: ContractDetailsMap,
  twitterDetailsMap: TwitterDetailsMap,
  collection: NftGo.Collection,
): TvsRanking => {
  const contractDetails = pipe(
    collection.contracts,
    A.map(O.fromNullableK((address) => contractDetailsMap.get(address))),
    A.compact,
    A.head,
  );
  const twitterDetails = pipe(
    contractDetails,
    O.chain(O.fromNullableK((details) => details.twitterHandle)),
    O.chain(
      O.fromNullableK((twitterHandle) => twitterDetailsMap.get(twitterHandle)),
    ),
  );

  return pipe(
    contractDetails,
    O.match(
      () => buildRankingFromCollection(collection),
      (contractDetails) =>
        pipe(
          twitterDetails,
          O.match(
            () =>
              buildRankingWithContractDetailsFromCollection(
                contractDetails,
                collection,
              ),
            (twitterDetails) =>
              buildRankingWithAllDetailsFromCollection(
                contractDetails,
                twitterDetails,
                collection,
              ),
          ),
        ),
    ),
  );
};

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
    TE.bind("contractDetailsMap", ({ rankedCollections }) =>
      pipe(
        rankedCollections,
        A.chain((collection) => collection.contracts),
        (addresses) => TE.fromTask(getContractDetailsForAddresses(addresses)),
      ),
    ),
    TE.map(({ contractDetailsMap, rankedCollections, twitterDetailsMap }) =>
      pipe(
        rankedCollections,
        A.filter((collection) => collection.blockchain === "ETH"),
        A.takeLeft(20),
        A.map((collection) =>
          tvsRankingFromNftCollection(
            contractDetailsMap,
            twitterDetailsMap,
            collection,
          ),
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
