import argparse
import utils


def check_tx(tx, maturity):
    return tx['maturity'] == maturity


def delete_loop(db, maturity, no_update):
    collection_name = 'users'
    user_ref = db.collection(collection_name)
    docs = user_ref.stream()

    for doc in docs:
        user_data = doc.to_dict()
        user_txs = user_data['txs']
        for tx_hash in user_txs.keys():
            if check_tx(user_txs[tx_hash], maturity):
                if not no_update:
                    v_print(f'[INFO] {tx_hash} to be removed.')
                else:
                    user_data.remove(tx_hash)
        doc.set(user_data)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--credentials', help = 'Path to Firebase credentials')
    parser.add_argument('--maturity', type = int, help = 'Maturity to prune transactions for')
    parser.add_argument('--no-update', action = 'store_true', help = 'No DB write for debug')
    parser.add_argument('--verbose', action = 'store_true', help = 'Verbose for debug')

    args = parser.parse_args()

    if args.no_update or args.verbose:
        def v_print(s):
            print(s)
    else:
        def v_print(s):
            return

    if not args.no_update:
        ans = input(f'[WARNING] Removing transactions for {args.maturity} maturity. Continue? (Y/N)')
        if ans != 'Y':
            print(f'[INFO] Transaction pruning cancelled. Exiting...')
            exit()

    db = utils.get_db(args.credentials)
    delete_loop(db, args.maturity, args.no_update)
