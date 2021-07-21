import json

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

from web3 import Web3
from web3.middleware import geth_poa_middleware

def set_endpoint(endpoint):
    w3 = Web3(Web3.HTTPProvider(endpoint))
    w3.middleware_onion.inject(geth_poa_middleware, layer = 0)

    return w3


def get_latest_block(w3):
    return w3.eth.block_number


def get_transaction(hash, w3):
    return w3.eth.get_transaction(hash)


def get_receipt(hash, w3):
    return w3.eth.get_transaction_receipt(hash)


def get_block(num, w3):
    return w3.eth.get_block(num)


def load_abi(file):
    with open(file, 'r') as f:
        abi = json.load(f)
    return abi


def convert_address(address):
    return Web3.toChecksumAddress(address)

def get_db(key):
    cred = credentials.Certificate(key)
    firebase_admin.initialize_app(cred)
    db = firestore.client()
    return db
