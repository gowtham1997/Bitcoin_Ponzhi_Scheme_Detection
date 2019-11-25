import json
from datetime import datetime
from collections import defaultdict, Counter
import numpy as np
from copy import deepcopy
import pandas as pd
import swifter
# from tqdm import tqdm
# from pprint import pprint

TRANSACTIONS_DIR = '../transactions/'
DATA_DIR = '../data/'

def read_json(address):
    with open(TRANSACTIONS_DIR + address + '.json') as f:
        data = json.load(f)
    return data


def gini(array):
    # source: https://github.com/oliviaguest/gini/blob/master/gini.py
    """Calculate the Gini coefficient of a numpy array."""
    # based on bottom eq:
    # http://www.statsdirect.com/help/generatedimages/equations/equation154.svg
    # from:
    # http://www.statsdirect.com/help/default.htm#nonparametric_methods/gini.htm
    # All values are treated equally, arrays must be 1d:
    if len(array) == 0:
        return -1.0
    array = array.flatten()
    if np.amin(array) < 0:
        # Values cannot be negative:
        array -= np.amin(array)
    # Values cannot be 0:
    array += 0.0000001
    # Values must be sorted:
    array = np.sort(array)
    # Index per array element:
    index = np.arange(1, array.shape[0] + 1)
    # Number of array elements:
    n = array.shape[0]
    # Gini coefficient:
    return ((np.sum((2 * index - n - 1) * array)) / (n * np.sum(array)))


def get_lifetime(dates):
    dates = [datetime.strptime(d, '%Y-%m-%d %H:%M:%S') for d in dates]
    start_date = min(dates)
    end_date = max(dates)

    return (end_date - start_date).total_seconds()


def get_unique_addresses(trans_in, trans_out):
    in_addresses = []
    out_addresses = []

    for t in trans_in:
        in_addresses.append(t['inputs'].keys())
    for t in trans_out:
        out_addresses.append(t['outputs'].keys())

    addresses = in_addresses + out_addresses

    uniq_in_addresses = np.unique(in_addresses)
    uniq_out_addresses = np.unique(out_addresses)
    addresses = np.unique(addresses)

    return len(uniq_in_addresses), len(uniq_out_addresses), len(addresses)\



def get_delay_attributes(trans_in, trans_out):

    dates_dict = {}

    in_dates = [datetime.strptime(
        t['time'], '%Y-%m-%d %H:%M:%S') for t in trans_in]
    out_dates = [datetime.strptime(
        t['time'], '%Y-%m-%d %H:%M:%S') for t in trans_out]

    last_in_date = None
    delays = []

    for in_date in in_dates:
        dates_dict[in_date] = 'in'
    for out_date in out_dates:
        dates_dict[out_date] = 'out'

    for date in sorted(dates_dict.keys()):
        if dates_dict[date] == 'in':
            last_in_date = date
        if last_in_date and dates_dict[date] == 'out':
            delays.append((date - last_in_date).total_seconds())
            last_in_date = None

    if delays:

        return np.min(delays), np.max(delays), \
            np.mean(delays), np.median(delays)
    else:
        return 0.0, 0.0, 0.0, 0.0


def get_mean_std_gini_max_balance_delta(addr, trans_in, trans_out):

    in_date_btc = defaultdict(float)
    out_date_btc = defaultdict(float)

    daily_balance = defaultdict(float)
    daily_balance_list = defaultdict(list)

    balance_deltas = []

    dates = []
    simplified_dates = []

    for t in trans_in:
        dates.append(t['time'])
        in_date_btc[t['time']] = float(t['outputs'][addr])

    for t in trans_out:
        dates.append(t['time'])
        if addr in t['inputs'].keys():
            out_date_btc[t['time']] += float(t['inputs'][addr])
        if addr in t['outputs'].keys():
            # accounting for change address
            out_date_btc[t['time']] -= float(t['inputs'][addr])

    # we might refer the in_date and out_date for dates not present in the
    # defaultdict which might add 0.0 values on queried dates
    # To avoid this lets take a copy of the processed in_date and out_date btcs
    final_in_date_btc = deepcopy(in_date_btc)
    final_out_date_btc = deepcopy(out_date_btc)

    dates = [datetime.strptime(d, '%Y-%m-%d %H:%M:%S') for d in dates]
    # dates in ascending order
    dates = sorted(dates)
    _bal = 0
    for date in dates:
        string_date = datetime.strftime(date, '%Y-%m-%d %H:%M:%S')
        simplified_date = string_date.split(' ')[0]
        simplified_dates.append(simplified_date)

        daily_balance[simplified_date] = (
            in_date_btc[string_date] - out_date_btc[string_date])
        _bal += daily_balance[simplified_date]

        # daily_balance_list[simplified_date].append(
        # daily_balance[simplified_date])
        daily_balance_list[simplified_date].append(_bal)

    for idx, curr_date in enumerate(simplified_dates):
        if idx == 0:
            bal_list = daily_balance_list[curr_date]
            max_bal = max(bal_list)
            min_bal = min(bal_list)
            balance_deltas.append(max_bal - min_bal)
            continue

        bals = []
        prev_date = simplified_dates[idx - 1]
        if prev_date == curr_date:
            continue

        bal_list = daily_balance_list[curr_date]
        max_bal = max(bal_list)
        min_bal = min(bal_list)
        bals.append(max_bal)
        bals.append(min_bal)

        date_diff = (datetime.strptime(curr_date, '%Y-%m-%d') -
                     datetime.strptime(prev_date, '%Y-%m-%d')).total_seconds()
        # if date diff is within 2 days
        if date_diff <= 86400:
            bal_list = daily_balance_list[prev_date]
            max_bal = max(bal_list)
            min_bal = min(bal_list)
            bals.append(max_bal)
            bals.append(min_bal)

        # print(bals, max(bals), min(bals))
        balance_deltas.append(max(bals) - min(bals))

    # print(final_in_date_btc)
    # print(final_out_date_btc)
    # pprint(daily_balance_list)
    # for idx, date in enumerate(daily_balance_list):
    #     print(date, daily_balance_list[date], balance_deltas[idx])
    if len(final_out_date_btc) > 0:
        mean_in_btc = np.mean(list(final_in_date_btc.values()))
        std_in_btc = np.std(list(final_in_date_btc.values()))
        gini_in_btc = gini(np.array(list(final_in_date_btc.values())))
        gini_out_btc = gini(np.array(list(final_out_date_btc.values())))
        mean_out_btc = np.mean(list(final_out_date_btc.values()))
        std_out_btc = np.std(list(final_out_date_btc.values()))
        max_balance_diff = max(balance_deltas)
    else:
        mean_in_btc = np.mean(list(final_in_date_btc.values()))
        std_in_btc = np.std(list(final_in_date_btc.values()))
        gini_in_btc = gini(np.array(list(final_in_date_btc.values())))
        gini_out_btc = 0.00000001
        mean_out_btc = 0.00000001
        std_out_btc = 0.00000001
        max_balance_diff = max(balance_deltas)

    return mean_in_btc, mean_out_btc, \
        std_in_btc, std_out_btc, \
        gini_in_btc, gini_out_btc, max_balance_diff


def get_transaction_attributes(address):
    transactions = read_json(address)
    total_received = float(transactions['total_received']) / 1e8
    total_sent = float(transactions['total_sent']) / 1e8

    incoming_transactions = []
    outgoing_transactions = []

    for transaction in transactions['txs']:
        is_outgoing_transaction = False
        inputs_dict = defaultdict(float)
        outputs_dict = defaultdict(float)

        transaction_id = transaction['tx_index']
        time = datetime.utcfromtimestamp(
            int(transaction['time'])).strftime('%Y-%m-%d %H:%M:%S')

        for t_in in transaction['inputs']:
            # coinbase coin generation
            if 'prev_out' not in t_in.keys():
                continue
            if 'addr' not in t_in['prev_out'].keys():
                continue
            addr = t_in['prev_out']['addr']
            value = t_in['prev_out']['value']
            inputs_dict[addr] += float(value) / 1e8

        for t_out in transaction['out']:
            if 'addr' not in t_out.keys():
                continue
            addr = t_out['addr']
            value = t_out['value']
            outputs_dict[addr] += float(value) / 1e8

        if address in inputs_dict.keys():
            is_outgoing_transaction = True

        if is_outgoing_transaction:
            outgoing_transactions.append({'tx_id': transaction_id,
                                          'time': time,
                                          'inputs': inputs_dict,
                                          'outputs': outputs_dict})
        else:
            incoming_transactions.append({'tx_id': transaction_id,
                                          'time': time,
                                          'inputs': inputs_dict,
                                          'outputs': outputs_dict})
    # print('len of incoming:', len(incoming_transactions))
    # print('len of outgoing:', len(outgoing_transactions))
    return incoming_transactions, outgoing_transactions,\
        total_received, total_sent


def get_address_attributes(row):
    address = row['address']
    # print(address)
    incoming_transactions, outgoing_transactions, \
        total_received, total_sent = get_transaction_attributes(address)

    num_incoming = len(incoming_transactions)
    num_outgoing = len(outgoing_transactions)

    if num_outgoing == 0:
        in_out_ratio = num_incoming / (num_outgoing + 0.0000000001)
    else:
        in_out_ratio = num_incoming / num_outgoing
    transactions = incoming_transactions + outgoing_transactions

    last_inc_transaction_time = incoming_transactions[0]['time']
    first_inc_transaction_time = incoming_transactions[-1]['time']

    if num_outgoing == 0:
        imp_dates = [last_inc_transaction_time, first_inc_transaction_time]
    else:
        last_out_transaction_time = outgoing_transactions[0]['time']
        first_out_transaction_time = outgoing_transactions[-1]['time']

        imp_dates = [last_out_transaction_time, first_out_transaction_time,
                     last_inc_transaction_time, first_inc_transaction_time]

    lifetime = get_lifetime(imp_dates)

    dates = [t['time'].split(' ')[0] for t in transactions]

    dates_counter = Counter(dates)

    active_days = len(dates_counter)
    max_daily_trans = dates_counter.most_common(1)[0][1]

    uniq_in_addresses, uniq_out_addresses,\
        uniq_addresses = get_unique_addresses(
            incoming_transactions, outgoing_transactions)

    min_delay, max_delay, \
        mean_delay, median_delay = get_delay_attributes(
            incoming_transactions, outgoing_transactions)

    mean_in_btc, mean_out_btc, \
        std_in_btc, std_out_btc, \
        gini_in_btc, gini_out_btc,\
        max_balance_diff = get_mean_std_gini_max_balance_delta(
            address, incoming_transactions, outgoing_transactions)

    features = {}
    features['lifetime'] = lifetime
    features['active_days'] = active_days
    features['max_daily_trans'] = max_daily_trans
    features['gini_in_btc'] = gini_in_btc
    features['mean_in_btc'] = mean_in_btc
    features['std_in_btc'] = std_in_btc
    features['gini_out_btc'] = gini_out_btc
    features['mean_out_btc'] = mean_out_btc
    features['std_out_btc'] = std_out_btc
    features['total_in_btc'] = total_received
    features['total_out_btc'] = total_sent
    features['num_incoming'] = num_incoming
    features['num_outgoing'] = num_outgoing
    features['in_out_ratio'] = in_out_ratio
    features['uniq_in_addresses'] = uniq_in_addresses
    features['uniq_out_addresses'] = uniq_out_addresses
    features['uniq_addresses'] = uniq_addresses
    features['min_delay'] = min_delay
    features['max_delay'] = max_delay
    features['mean_delay'] = mean_delay
    features['median_delay'] = median_delay
    features['max_balance_diff'] = max_balance_diff

    return features


df = pd.read_csv(DATA_DIR + 'addrs_and_trans.csv')
# only take the files which have been succesfully extracted from the api
df = df[df['transactions_generated'] == 1]
df = df.drop('transactions_generated', axis=1)

features = ['lifetime',
            'active_days',
            'max_daily_trans',
            'gini_in_btc',
            'mean_in_btc',
            'std_in_btc',
            'gini_out_btc',
            'mean_out_btc',
            'std_out_btc',
            'total_in_btc',
            'total_out_btc',
            'num_incoming',
            'num_outgoing',
            'in_out_ratio',
            'uniq_in_addresses',
            'uniq_out_addresses',
            'uniq_addresses',
            'min_delay',
            'max_delay',
            'mean_delay',
            'median_delay',
            'max_balance_diff']
df[features] = df.swifter.apply(
    get_address_attributes, axis=1, result_type="expand")
df.to_csv(DATA_DIR + 'features.csv', index=False)
