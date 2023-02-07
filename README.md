# ETH Analysis Ξ🔍

Services backing [ultrasound.money](https://ultrasound.money). Written in TypeScript.

## Dependencies

- an execution node (we use Geth)
- a consensus/beacon node (we use lighthouse)
- postgres
- etherscan API
- twitter API
- opensea API (slower without a key)
- coingecko API (no key needed)

## Usage

Install the library dependencies using `yarn install`.
To run the services various environment variables are required depending on the service being run. To take as example running the core block analysis, a way to supply the required env vars is to create a file `set-env-dev.sh` with the following contents:

```sh
export ENV=dev
export GETH_URL=**** # using the WebSocket port
export BEACON_URL=****
export LOG_LEVEL=debug
export PGDATABASE=****
export PGHOST=****
export PGPASSWORD=****
export PGPORT=****
export PGSSLMODE=prefer
export PGUSER=****
```

Then `source ./set-env-dev.sh` in your favorite shell to make them available. One would then run `node --loader ts-node/esm src/analyze_blocks.ts` to start block analysis. The service has many entry points like this. Note, block analysis would need many days to catch up from block 12965000, the london hard fork, to now, and start producing sensible analysis for all time frames.
