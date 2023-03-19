import { pipe, TO } from "../fp.js";
import * as BeaconRewards from "../beacon_rewards.js";
import * as BlockLag from "../block_lag.js";
import * as BurnCategories from "../burn-categories/burn_categories.js";
import * as Db from "../db.js";
import * as EffectiveBalanceSum from "../effective_balance_sum.js";
import * as EthPricesAverages from "../eth-prices/averages.js";
import * as EthSupplyParts from "../eth_supply_parts.js";
import * as GroupedAnalysis1 from "../grouped_analysis_1.js";
import * as IssuanceBreakdown from "../issuance_breakdown.js";
import * as KeyValueStore from "../key_value_store.js";
import * as Log from "../log.js";
import * as MarketCaps from "../market-caps/market_caps.js";
import * as MergeEstimate from "../merge_estimate.js";
import * as PeRatios from "../pe_ratios.js";
import * as ScarcityCache from "../scarcity/cache.js";
import * as SupplyProjection from "../supply-projection/supply_projection.js";
import * as TotalValueSecured from "../total-value-secured/total_value_secured.js";
import * as DateFns from "date-fns";

// Prepare caches before registering routes or even starting the server.
export const store = {
  scarcityCache: await ScarcityCache.getScarcityCache()(),
  groupedAnalysis1Cache: await GroupedAnalysis1.getLatestAnalysis()(),
  oMarketCapsCache: await MarketCaps.getStoredMarketCaps()(),
  burnCategoriesCache: await BurnCategories.getCategoriesCache()(),
  averagePricesCache: await EthPricesAverages.getAveragePricesCache()(),
  peRatiosCache: await PeRatios.getPeRatiosCache()(),
  oTotalValueSecuredCache:
    await TotalValueSecured.getCachedTotalValueSecured()(),
  blockLag: await KeyValueStore.getValue(BlockLag.blockLagCacheKey)(),
  validatorRewards: await KeyValueStore.getValue(
    BeaconRewards.validatorRewardsCacheKey,
  )(),
  oSupplyProjectionInputs: await KeyValueStore.getValue(
    SupplyProjection.supplyProjectionInputsCacheKey,
  )(),
  oIssuanceBreakdown: await IssuanceBreakdown.getIssuanceBreakdown()(),
  oEthSupplyParts: await pipe(
    KeyValueStore.getValueStr(EthSupplyParts.ethSupplyPartsCacheKey),
    TO.alt(() =>
      KeyValueStore.getValueStr(EthSupplyParts.ethSupplyPartsCacheKeyOld),
    ),
  )(),
  oEffectiveBalanceSum:
    await EffectiveBalanceSum.getLastEffectiveBalanceSum()(),
  oMergeEstimate: await KeyValueStore.getValueStr(
    MergeEstimate.MERGE_ESTIMATE_CACHE_KEY,
  )(),
};
Log.debug("loaded caches");

let lastSeenUpdate: Date | undefined = undefined;
const startedAt = new Date();

Db.sql.listen("cache-update", async (payload) => {
  Log.debug(`DB notify cache-update, cache key: ${payload}`);

  if (payload === undefined) {
    Log.error("DB cache-update with no payload, skipping");
    return;
  }

  lastSeenUpdate = new Date();

  if (payload === ScarcityCache.scarcityCacheKey) {
    store.scarcityCache = await ScarcityCache.getScarcityCache()();
    return;
  }

  if (payload === GroupedAnalysis1.groupedAnalysis1CacheKey) {
    store.groupedAnalysis1Cache = await GroupedAnalysis1.getLatestAnalysis()();
    return;
  }

  if (payload === MarketCaps.marketCapsCacheKey) {
    store.oMarketCapsCache = await MarketCaps.getStoredMarketCaps()();
    return;
  }

  if (payload === BurnCategories.burnCategoriesCacheKey) {
    store.burnCategoriesCache = await BurnCategories.getCategoriesCache()();
    return;
  }

  if (payload === EthPricesAverages.averagePricesCacheKey) {
    store.averagePricesCache =
      await EthPricesAverages.getAveragePricesCache()();
    return;
  }

  if (payload === PeRatios.peRatiosCacheKey) {
    store.peRatiosCache = await PeRatios.getPeRatiosCache()();
    return;
  }

  if (payload === TotalValueSecured.totalValueSecuredCacheKey) {
    store.oTotalValueSecuredCache =
      await TotalValueSecured.getCachedTotalValueSecured()();
    return;
  }

  if (payload === BlockLag.blockLagCacheKey) {
    store.blockLag = await KeyValueStore.getValue(BlockLag.blockLagCacheKey)();
    return;
  }

  if (payload === BeaconRewards.validatorRewardsCacheKey) {
    store.validatorRewards = await KeyValueStore.getValue(
      BeaconRewards.validatorRewardsCacheKey,
    )();
    return;
  }

  if (payload === SupplyProjection.supplyProjectionInputsCacheKey) {
    store.oSupplyProjectionInputs = await KeyValueStore.getValue(
      SupplyProjection.supplyProjectionInputsCacheKey,
    )();
    return;
  }

  if (payload === IssuanceBreakdown.issuanceBreakdownCacheKey) {
    store.oIssuanceBreakdown = await IssuanceBreakdown.getIssuanceBreakdown()();
    return;
  }

  if (
    payload === EthSupplyParts.ethSupplyPartsCacheKey ||
    payload === EthSupplyParts.ethSupplyPartsCacheKeyOld
  ) {
    store.oEthSupplyParts = await pipe(
      KeyValueStore.getValueStr(EthSupplyParts.ethSupplyPartsCacheKey),
      TO.alt(() =>
        KeyValueStore.getValueStr(EthSupplyParts.ethSupplyPartsCacheKeyOld),
      ),
    )();
    return;
  }

  if (payload === EffectiveBalanceSum.EFFECTIVE_BALANCE_SUM_CACHE_KEY) {
    store.oEffectiveBalanceSum =
      await EffectiveBalanceSum.getLastEffectiveBalanceSum()();
    return;
  }

  if (payload === MergeEstimate.MERGE_ESTIMATE_CACHE_KEY) {
    store.oMergeEstimate = await KeyValueStore.getValueStr(
      MergeEstimate.MERGE_ESTIMATE_CACHE_KEY,
    )();
    return;
  }
});

export const checkHealth = () => {
  if (lastSeenUpdate === undefined) {
    if (DateFns.differenceInSeconds(new Date(), startedAt) > 300) {
      throw new Error("no cache updates seen within 5 minutes since start");
    }

    // We haven't seen any updates yet, but we're still within 5 minutes from
    // start.
    return;
  }

  if (DateFns.differenceInSeconds(new Date(), lastSeenUpdate) > 300) {
    throw new Error("no cache updates seen within 5 minutes");
  }

  // We've seen an update within 5 minutes.
  return;
};
