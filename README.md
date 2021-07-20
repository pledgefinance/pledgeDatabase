# pledgeDatabase

Libraries required:
```
Web3
firebase_admin
```

Run example:
```
python update.py --network testnet --verbose
```
Currently only updates market data.

Requires `testnet.json` or `mainnet.json` service account keys

### Market Data
```
python3 update_market.py --endpoint https://data-seed-prebsc-1-s1.binance.org:8545/ --credentials ./testnet.json --interval 60
```
NOTE: Additional markets need to be added to DB manually & requires a script restart

### Token Prices

```
python3 update_price.py --endpoint https://data-seed-prebsc-1-s1.binance.org:8545/ --credentials ./testnet.json --interval 10
```
NOTE: Needs restart to update token list
