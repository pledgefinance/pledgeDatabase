import argparse
import math
import queue
import threading
import utils


market_abi_file = './abi/CashMarket.json'


def get_market_addresses(db):
    market_ref = db.collection('markets')
    docs = market_ref.stream()

    market_list = []
    for doc in docs:
        market_list.append(doc.id)

    return market_list

def get_trade_addresses(db):
    contract_ref = db.collection('contracts')
    docs = contract_ref.stream()

    trade_addresses = []
    for doc in docs:
        if doc.id == 'erc1155trade':
            trade_addresses.append(doc.to_dict()['address'])

    return trade_addresses


def process_block(block_num, w3, out):
    block = utils.get_block(block_num, w3)

    for tx in block.transactions:
        out.put((block.timestamp, tx))


def process_tx(data, valid_addresses, abi, w3, events, out):
    time, hash = data
    receipt = utils.get_receipt(hash, w3)

    if receipt.to is not None and receipt.to in valid_addresses:
        tx = {}
        tx['from'] = receipt['from']
        tx['to'] = receipt.to
        tx['block'] = receipt.blockNumber
        tx['time'] = time

        result = process_logs(receipt.to.lower(), abi, receipt, events, w3)
        if result is not None:
            name, maturity, fCash, cash = result
            tx['type'] = name
            tx['maturity'] = maturity
            tx['fCash'] = str(fCash)
            tx['cash'] = str(cash)
            if name == 'TakefCash' or name == 'TakeCurrentCash':
                tx['rate'] = str(((fCash - cash) / cash))
            else:
                tx['rate'] = 'N/A'
            # Note: Do not add transaction if does not contain a valid event log
            out.put((hash.hex(), tx))


valid_events = ['TakefCash', 'TakeCurrentCash', 'AddLiquidity', 'RemoveLiquidity']
def process_logs(address, abi, receipt, events, w3):
    address = '0xeeCa551d77e9CEf29883811df96B08BD01946ec9'
    contract = w3.eth.contract(utils.convert_address(address), abi = abi)

    result = None
    # Note: Exiting loop on first log for the CashMarket
    logs = receipt.logs
    for l in logs:
        if l['address'].lower() != address.lower():
            continue
        receipt_hex = w3.toHex(l['topics'][0])
        name = events.get(receipt_hex, None)

        if name is not None and name in valid_events:
            # Note: processReceipt returns a tuple with, may need to be changed for later
            decoded = contract.events[name]().processReceipt(receipt)[0]
            a = decoded.args
            result = (name, a.maturity, a.fCash, a.cash)
            break

    return result


def get_contract_events(address, abi, w3):
    contract = w3.eth.contract(utils.convert_address(address), abi = abi)
    events = [event for event in contract.abi if event['type'] == 'event']

    event_sigs = {}
    for e in events:
        name = e['name']
        inputs = [param['type'] for param in e['inputs']]
        inputs = ','.join(inputs)

        event_sig = f'{name}({inputs})'
        event_hex = w3.toHex(w3.keccak(text = event_sig))
        event_sigs[event_hex] = name

    return event_sigs


def update_tx(start, end, batch_size, w3, db, no_update):
    abi = utils.load_abi(market_abi_file)

    market_addresses = get_trade_addresses(db)
    contract_events = get_contract_events(market_addresses[0], abi, w3)

    trade_addresses = get_trade_addresses(db)

    total_blocks = end - start
    block_batches = math.ceil(total_blocks / batch_size)

    for i in range(block_batches):
        batch_start = start + (batch_size * i)
        batch_end = min(start + (batch_size * (i + 1)), end)

        block_threads = []
        receipt_queue = queue.Queue()
        for block_num in range(batch_start, batch_end):
            t = threading.Thread(target = process_block, args = (block_num, w3, receipt_queue))
            block_threads.append(t)

        for bt in block_threads:
            bt.start()
        for bt in block_threads:
            bt.join()

        tx_threads = []
        store_queue = queue.Queue()
        while not receipt_queue.empty():
            data = receipt_queue.get()
            t = threading.Thread(target = process_tx, args = (data, trade_addresses, abi, w3, contract_events, store_queue))
            tx_threads.append(t)

        total_txs = len(tx_threads)
        tx_batches = math.ceil(total_txs / batch_size)
        for j in range(tx_batches):
            tx_index_start = 0 + (batch_size * j)
            tx_index_end = min(0 + (batch_size * (j + 1)), total_txs - 1)

            current_threads = []
            for k in range(tx_index_start, tx_index_end):
                t = tx_threads[k]
                current_threads.append(t)

            for ct in current_threads:
                ct.start()
            for ct in current_threads:
                ct.join()

        tx_data = {}
        while not store_queue.empty():
            hash, tx = store_queue.get()
            sender = tx['from'].lower()
            if tx_data.get(sender, None) is None:
                tx_data[sender] = {}
                tx_data[sender]['txs'] = {}
            tx_data[sender]['txs'][hash] = tx

        for user in tx_data.keys():
            if not no_update:
                user_doc = db.collection('users').document(user).set(tx_data[user], merge = True)

        print(tx_data)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--endpoint', help = 'Blockchain endpoint to connect to')
    parser.add_argument('--credentials', help = 'Path to Firebase credentials')
    parser.add_argument('--start', type = int, help = 'Start block')
    parser.add_argument('--end', type = int, help = 'End block (not inclusive)')
    parser.add_argument('--batch-size', type = int, default = 20, help = 'Batch size')
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

    update_tx(args.start, args.end, args.batch_size, w3, db, args.no_update)
