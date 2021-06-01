import pandas as pd
from web3 import Web3

WEB3_PROVIDER_ADDRESS = 'http://192.168.1.104:8545'
CONTRACT_ADDRESSES_CSV = 'contract_address.csv'


def analyze_block_gas_usage(block_number):
    w3 = Web3(Web3.HTTPProvider(WEB3_PROVIDER_ADDRESS))
    block = w3.eth.get_block(block_number, True)
    block_trace = w3.provider.make_request(
        'debug_traceBlockByNumber', [Web3.toHex(block.number), {'tracer': "callTracer"}]
    )
    # drop the first level of depth
    block_trace_transactions = [each.get('result') for each in block_trace.get('result')]

    if block_trace_transactions:
        # format block data gasUsed from Hex to int
        gas_converted_transactions = [
            {'to': str(t['to']).lower(), 'gasUsed': Web3.toInt(hexstr=t['gasUsed'])} for t in block_trace_transactions]

        # aggregate, sum, and count all transaction data by to address
        block_transactions_df = pd.DataFrame(gas_converted_transactions)
        block_transactions = block_transactions_df.groupby(['to', ]).agg(
            gasUsed=('gasUsed', 'sum'), blocktransactions=('to', 'count'))

        # format contract addresses to catch inconsistencies before merging data
        contract_addresses = pd.read_csv(CONTRACT_ADDRESSES_CSV)
        contract_addresses['address'] = contract_addresses['address'].apply(lambda x: str(x).lower())

        #  merge transaction receipts with dapp contract addresses
        result = block_transactions.merge(contract_addresses, left_on='to', right_on='address')
    else:
        result = 'No Transactions'
    return result

def analyze_latest_block_gas():
    w3 = Web3(Web3.HTTPProvider(WEB3_PROVIDER_ADDRESS))
    block = w3.eth.get_block('latest')
    if not w3.eth.is_syncing():
        print('Block Number - {}'.format(block.number))
        print('Block Transaction Count - {}'.format(len(block.transactions)))
        print(analyze_block_gas_usage(block.number))
    else:
        print(w3.eth.is_syncing())


analyze_latest_block_gas()
