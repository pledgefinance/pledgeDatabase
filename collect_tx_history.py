import argparse
import math
import queue
import threading
import utils


market_abi_file = './abi/CashMarket.json'
market_address = '0x56fe9b91db8d72d6660ad4623459ccb72095cd4b'


def process_block(block_num, w3, out):
    block = utils.get_block(block_num, w3)

    for tx in block.transactions:
        out.put((block.timestamp, tx))


valid_addresses = [market_address]
def process_tx(data, w3, events, out):
    time, hash = data
    receipt = utils.get_receipt(hash, w3)

    if receipt.to is not None and receipt.to.lower() in valid_addresses:
        tx = {}
        tx['from'] = receipt['from']
        tx['to'] = receipt.to
        tx['block'] = receipt.blockNumber
        tx['time'] = time

        result = process_logs(market_address, receipt, events, w3)
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
def process_logs(address, receipt, events, w3):
    contract = w3.eth.contract(utils.convert_address(address), abi = abi)

    result = None
    # Note: Existing loop on first log for CashMarket
    logs = receipt.logs
    for l in logs:
        if l['address'] != market_address:
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


def get_contract_events(address, w3):
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
    abi = utils.load_abi(market_abi_file)

    contract_events = get_contract_events(market_address, w3)

    user_ref = db.collection('users')
    docs = user_ref.stream()

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
            data = receipt_queue.get()
            t = threading.Thread(target = process_tx, args = (data, w3, contract_events, store_queue))
            tx_threads.append(t)
        for tt in tx_threads:
            tt.start()
        for tt in tx_threads:
            tt.join()

        dataset = {}
        while not store_queue.empty():
            hash, tx = store_queue.get()
            sender = tx['from'].lower()
            if dataset.get(sender, None) is None:
                dataset[sender] = {}
                dataset[sender]['txs'] = {}
            dataset[sender]['txs'][hash] = tx

        for user in dataset.keys():
            doc_ref = user_ref.document(user)
            if not args.no_update:
                doc_ref.set(dataset[user], merge = True)
            else:
                v_print(f'[INFO] Skipping db update.')
                v_print(dataset)
