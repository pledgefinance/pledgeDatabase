import argparse
import json

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore


from web3 import Web3


def init_blockchain():
    if args.network == 'testnet':
        endpoint = 'https://data-seed-prebsc-1-s1.binance.org:8545/'
    if args.network == 'mainnet':
        endpoint = 'https://bsc-dataseed.binance.org/'
    v_print(f'Connecting to {args.network} using {endpoint}')
    return Web3(Web3.HTTPProvider(endpoint))


def load_abi(path):
    with open(path, 'r') as f:
        abi = json.load(f)
    return abi['abi']


def update_market(markets, abi, w3):
    updated_market = {}
    for k in markets.keys():

        updated_market[k] = {}

        contract_address = markets[k]['address']
        market_contract = w3.eth.contract(address = Web3.toChecksumAddress(contract_address), abi = abi)
        updated_market[k]['address'] = contract_address

        # Update active maturities
        maturities = market_contract.functions.getActiveMaturities().call()
        v_print(maturities)

        # Update each maturity's data
        for m in maturities:
            try:
                rate = market_contract.functions.getRate(m).call()
                v_print(rate)
                data = market_contract.functions.getMarket(m).call()
                v_print(data)
                updated_market[k]['maturities'] = {}
                updated_market[k]['maturities'][str(m)] = {}
                updated_market[k]['maturities'][str(m)]['rate'] = rate[0] * 1e-9
                updated_market[k]['maturities'][str(m)]['fCash'] = data[0] * 1e-18
                updated_market[k]['maturities'][str(m)]['currentCash'] = data[2]  * 1e-18
            except Exception as e:
                v_print(type(e))
                v_print(f'[Error] Maturity {m} not setup yet.')

    return updated_market


    return
    market_address = market.child('address'),get().val()
    market_contract = w3.eth.contract(market_address, abi)

    # Update active maturities
    maturities = market_contract.functions.getActiveMaturities().call()
    v_print(maturities)

    # Update each maturities rate & total numbers
    for e in maturities:
        rate = market_contract.functions.getRate().call()
        v_print(rate)

    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--network', choices = ['mainnet', 'testnet'] ,help = 'Testnet or Mainnet')
    parser.add_argument('--verbose', action = 'store_true', help = 'Verbose for debug')
    args = parser.parse_args()
    if args.verbose:
        def v_print(s):
            print(s)
    else:
        def v_print(s):
            return

    w3 = init_blockchain()

    v_print(f'Connecting to {args.network} Firebase')
    if args.network == 'testnet':
        cred = credentials.Certificate('./testnet.json')
    # TODO: Mainnet config
    if args.network == 'mainnet':
        cred = credentials.Certificate('./mainnet.json')

    firebase_admin.initialize_app(cred)
    db = firestore.client()

    market_abi = load_abi('./CashMarket.json')

    market_ref = db.collection('markets')
    docs = market_ref.stream()

    for doc in docs:
        v_print(f'{doc.id} => {doc.to_dict()}')
        updated_market = update_market(doc.to_dict(), market_abi, w3)
        v_print(updated_market)

        doc_ref = db.collection('markets').document(doc.id)
        doc_ref.set(updated_market, merge = True)
