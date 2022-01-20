# Burn Categories

## High Level

### MVP
For each category of contract, answer:
* total fees burned
* percent of all fees burned

### v2
* same for each time frame
* total transactions

## Technical

### MVP

Using SQL, takes 54.3s for current 19M fee rows. Climbing at a rate of 3.4M per month (18%).

```
SELECT
  SUM(base_fees) AS fees,
  SUM(base_fees * eth_price) AS fees_usd,
  SUM(transaction_count) AS transaction_count
FROM contract_base_fees
JOIN blocks ON number = block_number
JOIN contracts ON address = contract_address
WHERE category IS NOT NULL
GROUP BY (category)
```

Using remembered aggregates.
on init:
* find the next block to analyze.
* add fees from blocks to category aggregates.

on new block:
* add fees from blocks to category aggregates.

on rollback:
* remove fees from blocks from category aggregates.

on new category:
* add all fees from contract to category aggregate.
