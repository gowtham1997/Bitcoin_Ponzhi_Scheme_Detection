import pandas as pd

DATA_DIR = 'data/'

# all addresses csv and ponzi_32_csv obtained from
# Repo: https://github.com/bitcoinponzi/BitcoinPonziTool
# address in repo: CSV/AddressPonziOrNotTag.csv, Addresses.csv
all_addresses_df = pd.read_csv(DATA_DIR + 'all_addresses.csv')
print('All addresses csv shape: ', all_addresses_df.shape)
ponzi_addresses_df = pd.read_csv(DATA_DIR + 'ponzi_32.csv')
print('ponzi addresses csv shape: ', ponzi_addresses_df.shape)
# removing ponzi addresses from all_addresses df
# strangely these ponzi addresses do not correspond to the 32 ponzi addresses
# in another csv file(which are referenced in the orginal paper)
non_ponzi_addresses = all_addresses_df[
    all_addresses_df['isPonzi'] == False]['address'].to_list()

print('Non ponzi addresses: ', len(non_ponzi_addresses))

# take the 32 ponzi addresses mentioned in the paper
ponzi_addresses = ponzi_addresses_df['address'].to_list()

merged_addresses_df = pd.DataFrame(
    non_ponzi_addresses + ponzi_addresses, columns=['address'])

classes = [0] * len(non_ponzi_addresses) + [1] * len(ponzi_addresses)

merged_addresses_df['is_ponzi'] = classes

merged_addresses_df.to_csv(DATA_DIR + 'merged_addresses.csv', index=False)

print('Addresses merged!')
print('Merged addresses csv has shape:', merged_addresses_df.shape)
print(merged_addresses_df.head())
