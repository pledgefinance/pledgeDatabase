import argparse
import json
import sys

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

from web3 import Web3
from web3.middleware import geth_poa_middleware


def init_blockchain():
    if args.network == 'testnet':
        endpoint = 'https://data-seed-prebsc-1-s1.binance.org:8545/'
    if args.network == 'mainnet':
        endpoint = 'https://bsc-dataseed.binance.org/'
    v_print(f'Connecting to {args.network} using {endpoint}')

    w3 = Web3(Web3.HTTPProvider(endpoint))
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)

    return w3

def load_abi(path):
    with open(path, 'r') as f:
        abi = json.load(f)
    return abi


def latest_block(w3):
    return w3.eth.block_number


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
                updated_market[k]['maturities'][str(m)]['rate'] = str(rate[0])
                updated_market[k]['maturities'][str(m)]['fCash'] = str(data[0])
                updated_market[k]['maturities'][str(m)]['currentCash'] = str(data[2])
                # TODO: Change the hardcoded APR formula
                updated_market[k]['maturities'][str(m)]['apr'] = str(((rate[0] * 1e-9) - 1) * 12)
            except Exception as e:
                v_print(type(e))
                v_print(f'[Error] Maturity {m} not setup yet.')

    return updated_market


def update_transactions(start, end, valid_to, w3):
    v_print(valid_to)
    transactions = []
    for block_num in range(start, end):
        block = w3.eth.get_block(block_num)
        v_print(f'{block_num}')
        transactions.extend(block.transactions)

    users = {}
    for t in transactions:
        tx = w3.eth.get_transaction(t)
        v_print(tx.to)
        if tx.to in valid_to:
            sender = tx['from']
            hash = tx.hash.hex()
            if users.get(sender, None) is None:
                v_print(f'New user {sender}')
                users[sender] = {}
                users[sender]['txs'] = {}
            # TODO: What other transaction data is required?
            users[sender]['txs'][hash] = {}
            users[sender]['txs'][hash]['to'] = tx.to
            users[sender]['txs'][hash]['block'] = tx.blockNumber

    return users


def update_token_price(oracle_address, oracle_abi, w3):
    v_print(f'Updating token price: {oracle_address}')
    oracle_contract = w3.eth.contract(address = Web3.toChecksumAddress(oracle_address), abi = oracle_abi)
    price = oracle_contract.functions.latestRound().call()

    v_print(price)

    return price


def update_assets(users, portfolio_address, portfolio_abi, w3):
    v_print(f'Updating user assets')
    portfolio_contract = w3.eth.contract(address = Web3.toChecksumAddress(portfolio_address), abi = portfolio_abi)

    assets = {}
    for u in users:
        user_assets = portfolio_contract.functions.getAssets(u).call()
        v_print(user_assets)
        formatted_assets = []
        for a in user_assets:
            cashGroupID, _, maturity, assetType, _, value = a
            asset = {}
            asset['cashGroupID'] = cashGroupID
            asset['maturity'] = maturity
            asset['assetType'] = assetType.hex()
            asset['value'] = str(value)
            formatted_assets.append(asset)
        assets[u] = {}
        assets[u]['assets'] = formatted_assets

    return assets


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--network', choices = ['mainnet', 'testnet'] ,help = 'Testnet or Mainnet')
    parser.add_argument('--dryrun', action = 'store_true', help = 'No DB write for testing')
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

    market_abi = load_abi('./abi/CashMarket.json')
    portfolio_abi = load_abi('./abi/Portfolios.json')
    oracle_abi = load_abi('./abi/IAggregator.json')

    valid_contracts = []

    market_ref = db.collection('markets')
    docs = market_ref.stream()

    for doc in docs:
        v_print(f'{doc.id} => {doc.to_dict()}')
        market_dict = doc.to_dict()
        updated_market = update_market(market_dict, market_abi, w3)
        v_print(updated_market)

        for m in market_dict.keys():
            valid_contracts.append(market_dict[m]['address'])

        doc_ref = db.collection('markets').document(doc.id)
        if args.dryrun:
            print('[DRYRUN] Market data update')
        else:
            doc_ref.set(updated_market, merge = True)

    portfolio_address = ''

    contract_ref = db.collection('contracts')
    docs = contract_ref.stream()

    for doc in docs:
        v_print(f'{doc.id} => {doc.to_dict()}')
        # FIXME: determine which contracts should be stored
        valid_contracts.extend(list(doc.to_dict().values()))
        portfolio_address = doc.to_dict()['portfolio']

    v_print(valid_contracts)
    v_print(portfolio_address)

    checkpoint_block = 0

    metadata_ref = db.collection('metadata')
    docs = metadata_ref.stream()

    for doc in docs:
        v_print(f'{doc.id} => {doc.to_dict()}')
        checkpoint_block = doc.to_dict()['checkpoint']

    v_print(checkpoint_block)

    current_block = latest_block(w3)

    # txs = update_transactions(checkpoint_block, current_block, valid_contracts, w3)
    txs = update_transactions(10585235, 10585240, valid_contracts, w3)
    v_print(txs)
    if args.dryrun:
        print('[DRYRUN] User tx data update')
    else:
        user_ref = db.collection('users')
        docs = user_ref.stream()
        for doc in docs:
            v_print(f'{doc.id} => {doc.to_dict()}')
            doc_ref = db.collection('users').document(doc.id)
            doc_ref.set(txs, merge = True)

    users = txs.keys()
    assets = update_assets(users, portfolio_address, portfolio_abi, w3)

    if args.dryrun:
        print('[DRYRUN] User asset data update')
    else:
        user_ref = db.collection('users')
        docs = user_ref.stream()
        for doc in docs:
            v_print(f'{doc.id} => {doc.to_dict()}')
            doc_ref = db.collection('users').document(doc.id)
            doc_ref.set(assets, merge = True)

    docs = metadata_ref.stream()
    for doc in docs:
        v_print(f'{doc.id} => {doc.to_dict()}')
        doc_ref = db.collection('metadata').document(doc.id)
        if args.dryrun:
            print('[DRYRUN] Checkpoint update')
        else:
            doc_ref.set({'checkpoint': current_block}, merge = True)

    tokens_ref = db.collection('tokens')
    docs = tokens_ref.stream()

    for doc in docs:
        v_print(f'{doc.id} => {doc.to_dict()}')
        tokens = doc.to_dict()
        for t in tokens.keys():
            addr = tokens[t]['chainlink']
            tokens[t]['price'] = str(update_token_price(addr, oracle_abi, w3))

        doc_ref = tokens_ref.document(doc.id)
        if args.dryrun:
            print('[DRYRUN] Token price update')
        else:
            doc_ref.set(tokens, merge = True)
