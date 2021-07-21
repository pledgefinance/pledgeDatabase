import argparse
import math
import queue
import threading
import utils


def process_block(block_num, w3, out):
    block = utils.get_block(block_num, w3)

    for tx in block.transactions:
        v_print(tx)
        out.put(tx)


valid_addresses = ['0x56fe9b91db8d72d6660ad4623459ccb72095cd4b']
def process_tx(hash, w3, out):
    receipt = utils.get_receipt(hash, w3)

    v_print(f'{receipt.to} // {valid_addresses[0]}')
    if receipt.to is not None and receipt.to.lower() in valid_addresses:
        tx = {}
        tx['from'] = receipt['from']
        tx['hash'] = hash.hex()
        tx['to'] = receipt.to
        tx['block'] = receipt.blockNumber
        # TODO: Figure out how to read the tx logs

        out.put(tx)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--endpoint', help = 'Blockchain endpoint to connect to')
    parser.add_argument('--credentials', help = 'Path to Firebase credentials')
    parser.add_argument('--start', type = int, help = 'Start block')
    parser.add_argument('--end', type = int, help = 'End block (not inclusive)')
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

    user_ref = db.collection('users')
    docs = user_ref.stream()

    doc_id = ''
    for doc in docs:
        doc_id = doc.id

    batch_size = 20
    total_blocks = args.end - args.start
    num_intervals = math.ceil(total_blocks / batch_size)

    for i in range(num_intervals):
        curr_block = args.start + (batch_size * i)
        end_block = args.start + (batch_size * (i + 1))
        if end_block > args.end:
            end_block = args.end
        v_print(f'{i}: {curr_block}-{end_block}')

        block_threads = []
        receipt_queue = queue.Queue()
        for block_num in range(curr_block, end_block):
            t = threading.Thread(target = process_block, args = (block_num, w3, receipt_queue))
            block_threads.append(t)
        for bt in block_threads:
            bt.start()
        for bt in block_threads:
            bt.join()

        tx_threads = []
        store_queue = queue.Queue()
        while not receipt_queue.empty():
            tx_hash = receipt_queue.get()
            t = threading.Thread(target = process_tx, args = (tx_hash, w3, store_queue))
            tx_threads.append(t)
        for tt in tx_threads:
            tt.start()
        for tt in tx_threads:
            tt.join()

        dataset = {}
        while not store_queue.empty():
            tx = store_queue.get()
            sender = tx['from'].lower()
            if dataset.get(sender, None) is None:
                dataset[sender] = {}
                dataset[sender]['txs'] = {}
            dataset[sender]['txs'][tx['hash']] = {}
            dataset[sender]['txs'][tx['hash']]['to'] = tx['to']
            dataset[sender]['txs'][tx['hash']]['blockNum'] = tx['block']

        doc_ref = user_ref.document(doc_id)
        if not args.no_update:
            doc_ref.set(dataset, merge = True)
        else:
            v_print(f'[INFO] Skipping db update.')
            v_print(dataset)
