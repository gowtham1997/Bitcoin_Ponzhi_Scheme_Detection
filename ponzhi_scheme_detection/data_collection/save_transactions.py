import pandas as pd
import requests
import json
# https://stackoverflow.com/questions/45545110/how-do-you-parallelize-apply-on-pandas-dataframes-making-use-of-all-cores-on-o
import swifter
import os

DATA_DIR = '../data/'
# DATA_DIR = './'
TRANSACTIONS_DIR = '../transactions/'
# TRANSACTIONS_DIR = './new_transactions/'

MAX_DEGREE = 1000

saved_jsons = os.listdir(TRANSACTIONS_DIR)


def get_and_save_transaction(address):
    # only for 50 latest transactions
    # requires api key which sometimes takes more than 5 days to validate

    if address + '.json' in saved_jsons:
        # print('skipping')
        return 1
    # print('getting ', address)
    # 1MzNQ7HV8dQ6XQ52zBGYkCZkkWv2Pd3VG6
    url = 'https://blockchain.info/address/{}?format=json&offset='.format(
        address)
    offset = 0
    transactions = []
    try:

        data = requests.get(url + str(offset)).json()
        transactions.extend(data['txs'])
        num_transactions = int(data['n_tx'])
        if num_transactions > MAX_DEGREE:
            print(f'skipping {address}(for now) with {num_transactions}')
            return 0
        # print(f'getting {address} with {num_transactions}')

        while(offset < num_transactions):
            offset += 50
            d = requests.get(url + str(offset)).json()
            transactions.extend(d['txs'])

        data['txs'] = transactions
    except Exception:
        print(Exception)
        print(address)
        return 0
    with open(TRANSACTIONS_DIR + address + '.json', 'w') as f:
        json.dump(data, f)
        return 1
#
# Retrieve all bitcoin transactions for a Bitcoin address
#


def get_all_transactions(bitcoin_address):
    block_explorer_url = "https://blockexplorer.com/api/addrs/"
    transactions = []
    from_number = 0
    to_number = 50

    block_explorer_url_full = block_explorer_url + bitcoin_address + \
        "/txs?from=%d&to=%d" % (from_number, to_number)

    response = requests.get(block_explorer_url_full)

    try:
        results = response.json()
    except Exception:
        print("[!] Error retrieving bitcoin transactions. "
              " Please re-run this script.")
        return transactions

    if results['totalItems'] == 0:
        print("[*] No transactions for %s" % bitcoin_address)
        return transactions

    transactions.extend(results['items'])

    while len(transactions) < results['totalItems']:

        from_number += 50
        to_number += 50

        block_explorer_url_full = block_explorer_url + bitcoin_address + \
            "/txs?from=%d&to=%d" % (from_number, to_number)

        response = requests.get(block_explorer_url_full)

        results = response.json()

        transactions.extend(results['items'])

    print("[*] Retrieved %d bitcoin transactions." % len(transactions))

    return transactions


merged_addresses_df = pd.read_csv(DATA_DIR + 'merged_addresses.csv')
# merged_addresses_df = pd.read_csv(DATA_DIR + 'HYIP_dataset.csv')

merged_addresses_df['transactions_generated'] = \
    merged_addresses_df['address'].swifter.apply(get_and_save_transaction)

merged_addresses_df.to_csv(DATA_DIR + 'addrs_and_trans.csv', index=False)
# merged_addresses_df.to_csv(DATA_DIR + 'HYIP_addrs_and_trans.csv', index=False)
