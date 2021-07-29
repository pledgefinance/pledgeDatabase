import argparse
import time
import utils


oracle_abi_file = './abi/IAggregator.json'


def get_token_price(address, abi, w3):
    contract = w3.eth.contract(utils.convert_address(address), abi = abi)
    price = contract.functions.latestRound().call()

    v_print(f'{address}: {price}')

    return price


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--endpoint', help = 'Blockchain endpoint to connect to')
    parser.add_argument('--credentials', help = 'Path to Firebase credentials')
    parser.add_argument('--interval', type = int, help = 'Time between updates in seconds')
    parser.add_argument('--no-update', action = 'store_true', help = 'No DB write for debug')
    parser.add_argument('--verbose', action = 'store_true', help = 'Verbose for debug')

    args = parser.parse_args()

    if args.no_update or args.verbose:
        def v_print(s):
            print(s)
    else:
        def v_print(s):
            return

    w3 = utils.set_endpoint(args.endpoint)
    db = utils.get_db(args.credentials)
    abi = utils.load_abi(oracle_abi_file)

    dataset = {}
    doc_id = ''

    token_ref = db.collection('tokens')
    docs = token_ref.stream()
    for doc in docs:
        dataset[doc.id] = doc.to_dict()

    while True:
        for t in dataset.keys():
            if not dataset[t].get('chainlink', None) is None:
                oracle_addr = dataset[t]['chainlink']
                price = str(get_token_price(oracle_addr, abi, w3))

                data = {
                    'price': price,
                }

                if not args.no_update:
                    db.collection('tokens').document(t).set(data, merge = True)

        print('Sleeping...')
        time.sleep(args.interval)
