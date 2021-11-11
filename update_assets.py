import argparse
import math
import queue
import threading
import time
import utils


portfolio_abi_file = './abi/Portfolios.json'


def get_assets(user, address, w3, out):
    contract = w3.eth.contract(utils.convert_address(address), abi=abi)
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
    parser.add_argument('--endpoint', help='Blockchain endpoint to connect to')
    parser.add_argument('--credentials', help='Path to Firebase credentials')
    parser.add_argument('--interval', type=int,
                        help='Time between updates in seconds')
    parser.add_argument('--no-update', action='store_true',
                        help='No DB write for debug')
    parser.add_argument('--verbose', action='store_true',
                        help='Verbose for debug')

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

    portfolio = db.collection('contracts').document(
        'portfolios').get().to_dict()
    portfolio_address = portfolio['address']

    while True:
        user_ref = db.collection('users')
        docs = user_ref.stream()

        users = []
        for doc in docs:
            if doc.id.startswith('0x'):
                users.append(doc.id)

        asset_threads = []
        asset_queue = queue.Queue()
        for user in users:
            t = threading.Thread(target=get_assets, args=(
                user, portfolio_address, w3, asset_queue))
            asset_threads.append(t)

        batch_size = 10
        total_users = len(asset_threads)
        num_batches = math.ceil(total_users / batch_size)
        for i in range(num_batches):
            start = 0 + (batch_size * i)
            end = min(0 + (batch_size * (i + 1)), total_users)
            current_threads = []
            for j in range(start, end):
                asset_threads[j].start()
                current_threads.append(asset_threads[j])
            for ct in current_threads:
                ct.join()

        while not asset_queue.empty():
            user, assets = asset_queue.get()
            data = {'assets': assets}
            doc_ref = user_ref.document(user)
            if not args.no_update:
                doc_ref.set(data, merge=True)
            else:
                v_print(f'[INFO] Skipping db update {user}')

        v_print('Sleeping...')
        time.sleep(args.interval)
