import argparse
import queue
import threading
import time
import utils


portfolio_abi_file = './abi/Portfolios.json'


def get_assets(user, address, w3, out):
    contract = w3.eth.contract(utils.convert_address(address), abi = abi)
    assets = contract.functions.getAssets(utils.convert_address(user)).call()

    formatted_assets = []
    for a in assets:
        cashGroupID, _, maturity, assetType, _, value = a
        asset = {}
        asset['cashGroupID'] = cashGroupID
        asset['maturity'] = maturity
        asset['assetType'] = assetType.hex()
        asset['value'] = str(value)
        formatted_assets.append(asset)

    v_print(f'{user}: {formatted_assets}')
    out.put((user, formatted_assets))


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
    abi = utils.load_abi(portfolio_abi_file)
    # TODO: Change this address when mainnet
    portfolio_address = '0x140691DDAF73942326fEae1Bb1720799d38198dB'

    while True:
        users = {}

        user_ref = db.collection('users')
        docs = user_ref.stream()
        doc_id = ''
        for doc in docs:
            users = doc.to_dict()
            doc_id = doc.id

        asset_threads = []
        asset_queue = queue.Queue()
        for user in users.keys():
            t = threading.Thread(target = get_assets, args = (user, portfolio_address, w3, asset_queue))
            asset_threads.append(t)
        for at in asset_threads:
            at.start()
        for at in asset_threads:
            at.join()

        user_assets = {}
        while not asset_queue.empty():
            user, assets = asset_queue.get()
            user_assets[user] = {}
            user_assets[user]['assets'] = assets

        doc_ref = user_ref.document(doc_id)
        if not args.no_update:
            doc_ref.set(user_assets, merge = True)
        else:
            v_print(f'[INFO] Skipping db update')
            v_print(users)

        v_print('Sleeping...')
        time.sleep(args.interval)
