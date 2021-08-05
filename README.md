# pledgeDatabase

Libraries required:
```
Web3
firebase_admin
```

### Market Data

```
python3 update_market.py --endpoint https://data-seed-prebsc-1-s1.binance.org:8545/ --credentials ./testnet.json --interval 60
```
NOTE: Markets need to be manually added to the database

### Token Prices

```
python3 update_price.py --endpoint https://data-seed-prebsc-1-s1.binanceo.rg:8545/ --credentials ./testnet.json --interval 10
```
NOTE: Token list needs to be manually added to the database

### User Assets

```
python3 update_assets.py --endpoint https://data-seed-prebsc-1-s1.binance.org:8545/ --credentials ./testnet.json --interval 60
```

### Tx History

Single historical fetch
```
python3 collect_tx_history.py --endpoint https://data-seed-prebsc-1-s1.binance.org:8545/ --credentials ./testnet.json --start $STARTBLOCK --end $ENDBLOCK
```

Constant update loop
```
python3 update_tx.py --endpoint https://data-seed-prebsc-1-s1.binance.org:8545/ --credentials ./testnet.json --interval 600
```

#### TODO
`init_db.py`: Create initial db structure from contract deployment files & token list file
`delete_old_transactions.py`: Scrubs db of tx history for old maturities
