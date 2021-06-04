from timeit import default_timer as timer
import pandas as pd
from web3 import Web3

WEB3_PROVIDER_ADDRESS = 'http://192.168.1.104:8545'
CONTRACT_ADDRESSES_CSV = 'exploring/contract_addresses.csv'

def analyze_block_transaction_recipts(block_number):
    w3 = Web3(Web3.HTTPProvider(WEB3_PROVIDER_ADDRESS))
    block = w3.eth.get_block(block_number, True)
    block_trx_receitps = [w3.eth.get_transaction_receipt(each.hash) for each in block.transactions]
    block_trx_data = [
        {'hash': each.get('hash'),'to': str(each.get('to')).lower(),
        'gasUsed':each.get('gasUsed')} for each in block_trx_receitps]
    return block_trx_data

def analyze_latest_block_gas():
    w3 = Web3(Web3.HTTPProvider(WEB3_PROVIDER_ADDRESS))
    block = w3.eth.get_block('latest', True)
    if not w3.eth.is_syncing():

        start_time = timer()
        block_trx_data = analyze_block_transaction_recipts(block.number)
        end_time = timer()
        block_trx_df = pd.DataFrame(block_trx_data)
        block_trx_summary = block_trx_df.groupby(['to', ]).agg(
                gasUsed=('gasUsed', 'sum'), trxs=('to', 'count'))
        simple_trx = block_trx_df['gasUsed'] == 21000
        simple_trx_gas_used = simple_trx.sum() * 21000

        # format contract addresses to catch inconsistencies before merging data
        contract_addresses = pd.read_csv(CONTRACT_ADDRESSES_CSV)
        contract_addresses['address'] = contract_addresses['address'].apply(lambda x: str(x).lower())
        # drop any duplicates, keep the first
        contract_addresses = contract_addresses.drop_duplicates(subset='address', keep='first')

        #  merge transaction receipts with dapp contract addresses
        result = block_trx_summary.merge(contract_addresses, left_on='to', right_on='address')

        print('\n=== Block Info ===')
        print('Block Number - {}'.format(block.number))
        print('Block Transactions - {}'.format(len(block.transactions)))
        print('\n=== Simple Transaction Gas Used ===')
        print(simple_trx_gas_used)

        print('\n=== Dapp Contract Gas Used ===')
        print(result)

        print('\n=== Dapp Summary ===')
        dapp_grouped = result.groupby(['dapp',]).agg(
            gasUsed=('gasUsed', 'sum'), trxs=('trxs', 'sum'))
        dapp_gas_used = dapp_grouped['gasUsed'].sum()
        print('Dapp Gas Used - {}'.format(dapp_gas_used))
        print(dapp_grouped)

        print('\n=== Debug Info ===')
        print('Block Number - {}'.format(block.number))
        print('Block Transactions - {}'.format(len(block.transactions)))
        print('Pulled Block Data in {:.3f}s'.format(end_time - start_time))
        print('Total Block Gas Used - {}'.format(block.get('gasUsed')))
        print('Simple Transaction Gas Used - {}'.format(simple_trx_gas_used))
        print('Dapp Block Gas Used - {}'.format(dapp_grouped['gasUsed'].sum()))
        print('Simple Transaction Gas Used Percent - {:.0%}'.format((simple_trx_gas_used / block.get('gasUsed'))))
        print('Dapp Gas Used Percent - {:.0%}'.format((dapp_gas_used / block.get('gasUsed'))))
        print('Attributed Gas Used Percent - {:.0%}'.format(((dapp_gas_used + simple_trx_gas_used) / block.get('gasUsed'))))

    else:
        print(w3.eth.is_syncing())

analyze_latest_block_gas()
