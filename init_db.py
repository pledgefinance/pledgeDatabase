import argparse
import json
import utils


valid_contracts = ['escrow', 'portfolios', 'erc1155', 'erc1155trade']


def setup_contracts(contracts, db):
    print(f'Setting up contract collection')

    collection_name = 'contracts'
    for k in contracts.keys():
        if k in valid_contracts:
            doc = {
                'address': contracts[k]
            }
            db.collection(collection_name).document(k).set(doc)


def update_metadata(contracts, db):
    print(f'Setting checkpoint to contract start block')

    checkpoint_doc = {
        'checkpoint': contracts['startBlock']
    }

    collection_name = 'metadata'
    metadata_ref = db.collection(collection_name)
    doc = metadata_ref.document('checkpoint').set(checkpoint_doc)


num_markets = 1
portfolio_abi_file = './abi/Portfolios.json'


def update_markets(contracts, w3, db):
    print(f'Getting market info')

    abi = utils.load_abi(portfolio_abi_file)
    address = contracts['portfolios']

    contract = w3.eth.contract(utils.convert_address(address), abi=abi)
    # Change the ids for later
    ids = list(range(1, num_markets + 1))
    cashGroups = contract.functions.getCashGroups(ids).call()

    collection_name = 'markets'

    for group in cashGroups:
        market_doc = {
            'maturityLength': group[1],
            'address': group[3],
            'currencyId': group[4],
            'maturities': {}
        }
        db.collection(collection_name).document(group[3]).set(market_doc)


token_list_file = './tokens.json'


def setup_tokens(db):
    print(f'Setting up tokens')

    tokens = utils.load_abi(token_list_file)

    collection_name = 'tokens'
    for t in tokens['tokens']:
        db.collection(collection_name).document(t['symbol']).set(t)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--credentials', help='Path to Firebase credentials')
    parser.add_argument('--endpoint', required=False,
                        help='Blockchain endpoint to connect to')
    parser.add_argument('--deploy-json', required=False,
                        help='Path to inital setup data (Not required for reset)')
    parser.add_argument('--token-json', required=False,
                        help='Path to token setup data (Not required for reset)')
    parser.add_argument('--reset', action='store_true',
                        help='Reset Firebase DB')

    args = parser.parse_args()

    w3 = utils.set_endpoint(args.endpoint)
    db = utils.get_db(args.credentials)

    metadata_ref = db.collection('metadata')
    metadata = metadata_ref.document('network').get()
    type = metadata.to_dict()['network']

    if args.reset:
        ans = input(f'[WARNING] Resetting {type} DB. Continue? (Y/N)')
        if ans == 'Y':
            print(f'[INFO] Reset confirmed. Processing...')
            # Do reset
            print(f'[INFO] Resets finished. Exiting...')
        else:
            print(f'[INFO] Reset canceled. Exiting...')
        exit()

    if not args.deploy_json:
        print(f'[ERROR] --deploy-json required for inital db setup.')
        exit()

    if not args.token_json:
        print(f'[ERROR] --token-json required for initial db setup.')

    print(f'[INFO] Setting up {type} DB.')

    contracts = utils.load_abi(args.deploy_json)
    setup_contracts(contracts, db)
    update_metadata(contracts, db)
    update_markets(contracts, w3, db)
    setup_tokens(db)
