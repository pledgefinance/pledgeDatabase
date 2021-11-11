import argparse
import time
import utils

market_abi_file = './abi/CashMarket.json'


def get_market(maturity, address, abi, w3):
    contract = w3.eth.contract(utils.convert_address(address), abi=abi)
    data = contract.functions.getMarket(maturity).call()

    return data


def get_rate(maturity, address, abi, w3):
    contract = w3.eth.contract(utils.convert_address(address), abi=abi)
    rate = contract.functions.getRate(maturity).call()

    # Note: getRate returns (rate, bool), only return the rate value
    return rate[0]


def get_active_maturities(address, abi, w3):
    contract = w3.eth.contract(utils.convert_address(address), abi=abi)
    active = contract.functions.getActiveMaturities().call()

    return active


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
    abi = utils.load_abi(market_abi_file)

    dataset = {}
    doc_id = ''

    while True:
        market_ref = db.collection('markets')
        docs = market_ref.stream()

        dataset = {}

        for doc in docs:
            dataset[doc.id] = doc.to_dict()

        for market in dataset.keys():
            market_address = dataset[market]['address']
            active_maturities = get_active_maturities(market_address, abi, w3)

            for maturity in active_maturities:
                updated_maturity = {}
                m = str(maturity)
                if dataset[market]['maturities'].get(m, None) is None:
                    v_print(f'New maturity {m}')
                    dataset[market]['maturities'][m] = {}
                updated_maturity['active'] = True
                try:
                    rate = get_rate(maturity, market_address, abi, w3)
                    updated_maturity['rate'] = str(rate)
                    # FIXME: Update hardcoded APR fomula
                    updated_maturity['apr'] = str(((rate * 1e-9) - 1) * 12)

                    data = get_market(maturity, market_address, abi, w3)
                    updated_maturity['fCash'] = str(data[0])
                    updated_maturity['liquidity'] = str(data[1])
                    updated_maturity['currentCash'] = str(data[2])
                    updated_maturity['rateAnchor'] = str(data[3])
                    updated_maturity['rateScalar'] = str(data[4])
                    updated_maturity['lastImpliedRate'] = str(data[5])
                except Exception as e:
                    v_print(f'[ERROR] Maturity {m} not set up')

                dataset[market]['maturities'][m] = updated_maturity

            # Note: Never been tested
            for maturity in dataset[market]['maturities'].keys():
                if int(maturity) not in active_maturities:
                    dataset[market]['maturities'][maturity]['active'] = False

        for k in dataset.keys():
            if not args.no_update:
                market_ref.document(k).set(dataset[k], merge=True)
            else:
                v_print(f'[INFO] Skipping db update for {k}')

        print('Sleeping...')
        time.sleep(args.interval)
