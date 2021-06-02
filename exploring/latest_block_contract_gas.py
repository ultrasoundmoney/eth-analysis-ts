from timeit import default_timer as timer
import pandas as pd
from web3 import Web3

WEB3_PROVIDER_ADDRESS = 'http://192.168.1.104:8545'
CONTRACT_ADDRESSES_CSV = 'exploring/contract_address.csv'


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
        print('=== Block Info ===')
        print('Block Number - {}'.format(block.number))
        print('Block Transaction Count - {}'.format(len(block.transactions)))
        start_time = timer()
        result = analyze_block_gas_usage(block.number)
        dapp_grouped = result.groupby(['dapp',]).agg(
            gasUsed=('gasUsed', 'sum'), blocktransactions=('blocktransactions', 'sum'))
        end_time = timer()

        print('\n=== Known Contract Gas Used ===')
        print(result)
        print('\n=== Summary ===')
        print(dapp_grouped)
        known_gasUsed = dapp_grouped['gasUsed'].sum()

        print('\n=== Debug Info ===')
        print('Analyzed Block Data in {:.3f}s'.format(end_time - start_time))
        print('Total Block Gas Used - {}'.format(block.get('gasUsed')))
        print('Known Block Gas Used - {}'.format(known_gasUsed))
        known_gasUsed_percent = 'Known Gas Used Percent - {:.0%}'.format((known_gasUsed / block.get('gasUsed')))
        print(known_gasUsed_percent)
    else:
        print(w3.eth.is_syncing())


analyze_latest_block_gas()
