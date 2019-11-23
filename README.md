**Ponzhi Classfication**

The folder `ponzhi_scheme_detection` contains all the files to generate the dataset and run the ponzhi_scheme_detection.


MAIN_DIR = `ponzhi_scheme_detection/`
DATA_DIR = `ponzhi_scheme_detection/data/`
TRANSACTIONS_DIR = `ponzhi_scheme_detection/transactions/`

Steps:
1. `cd ../ponzhi_scheme_detection`
2. The `all_addresses.csv` and `ponzi_32.csv` are taken from `https://github.com/bitcoinponzi/BitcoinPonziTool/tree/master/CSV`
3. Run `python merge_addresses` located in MAIN_DIR to generate `merged_addresses.csv` in the DATA_DIR
4. cd `data_collection`
5. Run `python save_transactions.py` to generate all the public addresses json files in the TRANSACTIONS_DIR
6. Since 5. takes a lot of time, it is best to change `MAX_DEGREE` hyperparameter to dowload transaction details of addresses with less transactions first. I have downloaded all the addresses with transactions less than 25k.
7. 6. also generates a CSV in the DATA_DIR keeping track of files that have been succesfully downloaded
8. cd `../feature_generation` and run `get_features.py` to generate `features.csv` in the DATA_DIR
9. cd `../` and run the notebook `Features_EDA_and_data_transform.ipynb` to do EDA and generate transformed features
10. run `Classfication.ipynb` to train models for the transformed features

