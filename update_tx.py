import argparse
import time
import utils

from collect_tx_history import update_tx


def get_checkpoint(db):
    data = db.collection('metadata').document('checkpoint').get()
    return data.to_dict()['checkpoint']


def update_checkpoint(checkpoint, db):
    data = {
        'checkpoint': checkpoint
    }
    db.collection('metadata').document('checkpoint').set(data)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--endpoint', help = 'Blockchain endpoitn to connect to')
    parser.add_argument('--credentials', help = 'Path to Firebase credentials')
    parser.add_argument('--interval', type = int, help = 'Time between updates in seconds')
    parser.add_argument('--batch_size', type = int, default = 20, help = 'Batch size')
    parser.add_argument('--verbose', action = 'store_true', help = 'Verbose for debug')
    parser.add_argument('--no-update', action = 'store_true', help = 'No DB write for debug')

    args = parser.parse_args()

    if args.no_update or args.verbose:
        def v_print(s):
            print(s)
    else:
        def v_print(s):
            return

    w3 = utils.set_endpoint(args.endpoint)
    db = utils.get_db(args.credentials)

    while True:
        checkpoint = get_checkpoint(db)
        v_print(f'Checkpoint block: {checkpoint}')

        current_block = utils.get_latest_block(w3)
        v_print(f'Current block: {current_block}')

        update_tx(checkpoint, current_block, args.batch_size, w3, db, args.no_update)

        update_checkpoint(current_block, db)

        time.sleep(args.interval)
