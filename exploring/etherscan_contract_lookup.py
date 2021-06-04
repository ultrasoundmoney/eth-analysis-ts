import csv
import random
import re
import requests
import time
import pandas as pd
from bs4 import BeautifulSoup

CONTRACT_ADDRESSES_CSV = 'exploring/contract_address.csv'

def random_user_agent():
    uastrings = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"\
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.111 Safari/537.36",\
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.72 Safari/537.36",\
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10) AppleWebKit/600.1.25 (KHTML, like Gecko) Version/8.0 Safari/600.1.25",\
        "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:33.0) Gecko/20100101 Firefox/33.0",\
        "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.111 Safari/537.36",\
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.111 Safari/537.36",\
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit/600.1.17 (KHTML, like Gecko) Version/7.1 Safari/537.85.10",\
        "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko",\
        "Mozilla/5.0 (Windows NT 6.3; WOW64; rv:33.0) Gecko/20100101 Firefox/33.0",\
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.104 Safari/537.36"\
    ]
    return random.choice(uastrings)

def get_contract_name_from_etherscan(contract_address):
    url = 'https://etherscan.io/address/{}/'.format(contract_address)
    headers = {'User-Agent': random_user_agent()}
    page = requests.get(url, headers=headers)
    soup = BeautifulSoup(page.content, 'html.parser')
    clean_title = re.sub(r"[\n\t\r]*", "", soup.title.string)

    if re.search(':', clean_title):
        dapp_name = clean_title.split(':')[0]
        contract_title = clean_title.split('|')[0].strip()
        return {'dapp': dapp_name, 'contract': contract_title, 'address': contract_address}
    elif re.search('Address', clean_title):
        return {'dapp': 'Unknown', 'contract': 'Unknown', 'address': contract_address}
    else:
        contract_title = clean_title.split('|')[0].strip()
        return {'dapp': contract_title, 'contract': contract_title, 'address': contract_address}

def create_contract_address_list():
    contract_addresses = pd.read_csv(CONTRACT_ADDRESSES_CSV)

    etherscan_contract_addresses_data = []
    for index, row in contract_addresses.iterrows():
        result = get_contract_name_from_etherscan(row['address'])
        etherscan_contract_addresses_data.append(result)
        # continously write to a csv file so we can track progress if fails
        with open('exploring/new_list.csv', 'a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([result.get('dapp', ''), result.get('contract', ''), result.get('address','')])
        time.sleep(0.25)

create_contract_address_list()


