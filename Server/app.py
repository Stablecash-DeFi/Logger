import bottle
bottle.BaseRequest.MEMFILE_MAX =  1024 * 1024

from bottle import Bottle, request, response
import os
import hashlib
from pymongo import MongoClient
from datetime import datetime, timezone

app = Bottle()

client = MongoClient('mongodb://mongodb:27017/')
db = client['json_db']

def convert_market_rate(amount, from_currency, to_currency, fiat_prices):
    if (from_currency == "EUR"):
        amount *= float(fiat_prices[from_currency])
    elif (to_currency == "EUR"):
        amount /= float(fiat_prices[to_currency])
    return amount

def rentability(amount, from_currency, to_currency, fiat_prices, rate):
    swap_gain = amount * rate - amount
    if (from_currency == 'EUR' and to_currency != 'EUR') or (to_currency == 'EUR' and from_currency != 'EUR'):
        corelleration = amount * rate - convert_market_rate(amount, from_currency, to_currency, fiat_prices)
        swap_gain = convert_market_rate(corelleration, to_currency, 'USD', fiat_prices)
    if from_currency == to_currency == 'EUR':
        swap_gain = convert_market_rate(swap_gain, from_currency, 'USD', fiat_prices)
    return swap_gain

def wallet_value(wallet, fiat_prices):
    ret = {"by_chain": {}, "total": 0}
    for chain in ["137", "solana"]:
        ret["by_chain"][chain] = {
            "EUR": sum([wallet[i] for i in {i: wallet[i] for i in wallet if i.split(":")[0] == chain} if i[-3:] == 'EUR' ]),
            "USD": sum([wallet[i] for i in {i: wallet[i] for i in wallet if i.split(":")[0] == chain} if i[-3:] == 'USD']),
            "total": None
        }
        ret["by_chain"][chain]["total"] = ret["by_chain"][chain]["USD"] + convert_market_rate(ret["by_chain"][chain]["EUR"], "EUR", "USD", fiat_prices)
        ret["total"] += ret["by_chain"][chain]["total"]
    return ret

@app.post('/')
def receive_json():
    auth_token = request.headers.get('Authorization')
    expected_token = os.getenv('API_KEY')
    if auth_token != f'Bearer {expected_token}':
        response.status = 401
        return {'error': 'Unauthorized'}
    dt = int(datetime.now().replace(tzinfo=timezone.utc).timestamp())
    data = request.json
    if data:
        if isinstance(data, list):
            for i in range(len(data)):
                data[i]["trade"]["swapConfig"]["fromAmount"] = int(data[i]["trade"]["swapConfig"]["fromAmount"]) / pow(10, data[i]["trade"]["swapConfig"]["fromDigits"])
                data[i]["trade"]["swapConfig"]["toAmount"] = int(data[i]["trade"]["swapConfig"]["toAmount"]) / pow(10, data[i]["trade"]["swapConfig"]["toDigits"])
                del data[i]["trade"]["swapConfig"]["fromDigits"]
                del data[i]["trade"]["swapConfig"]["toDigits"]
                data[i]["trade"]["swapConfig"]["gasCosts"] = [float(f'{float(cost["amountUsd"]):.6f}') for cost in data[i]["trade"]["swapConfig"]["gasCosts"]]
                data[i]["trade"]["swapConfig"]["feeCosts"] = [float(f'{float(cost["amountUsd"]):.6f}') for cost in data[i]["trade"]["swapConfig"]["feeCosts"]]

                data[i]["trade"]["pair"]["from"] = f'{data[i]["trade"]["pair"]["from"]["chain"]}:{data[i]["trade"]["pair"]["from"]["token"]}:{data[i]["trade"]["pair"]["from"]["currency"]}'
                data[i]["trade"]["pair"]["to"] = f'{data[i]["trade"]["pair"]["to"]["chain"]}:{data[i]["trade"]["pair"]["to"]["token"]}:{data[i]["trade"]["pair"]["to"]["currency"]}'

                REF = {}
                for j in range(len(data[i]["wallet"])):
                    index = f'{data[i]["wallet"][j]["chain"]}:{data[i]["wallet"][j]["address"]}:{data[i]["wallet"][j]["currency"]}'
                    REF[index] = float(f'{data[i]["wallet"][j]["amount"]:.6f}')
                data[i]["wallet"] = REF

                d = {
                    "cost": {
                        "gas": data[i]["trade"]["swapConfig"]["gasCosts"],
                        "fee": data[i]["trade"]["swapConfig"]["feeCosts"],
                        "total": data[i]["trade"]["swapConfig"]["transactionCost"]
                    },
                    "exchange": {
                        "rate":  float(f'{data[i]["trade"]["swapConfig"]["exchangeRate"]:.6f}'),
                        "from": data[i]["trade"]["pair"]["from"],
                        "to": data[i]["trade"]["pair"]["to"]
                    },
                    "price": {
                        "USD": 1.00,
                        "EUR": data[i]["trade"]["swapConfig"]["fiatPrices"]["USD"],
                        "SOL": data[i]["trade"]["solanaPrice"],
                        "MAT": data[i]["trade"]["maticPrice"]
                    },
                    "rentability": None,
                    "timestamp": dt
                }
                renta = rentability(
                    amount = 100,
                    from_currency = d["exchange"]["from"][-3:],
                    to_currency = d["exchange"]["to"][-3:],
                    fiat_prices = d["price"],
                    rate = d["exchange"]["rate"]
                )
                d["rentability"] = float(f"{renta:.6f}")

                wallet = {
                    "_id": None,
                    "data": {
                        "stable_coins": data[i]["wallet"],
                        "value": None,
                    },
                    "timestamp": dt
                }
                wallet["data"]["value"] = wallet_value(wallet["data"]["stable_coins"], d["price"])
                wallet["_id"] = hashlib.sha256(str(wallet["data"]).encode()).hexdigest()

                db['trades'].insert_one({"json_data": d})
                db['wallets'].insert_one({"json_data": wallet})

        return {'error': None}
    else:
        return {'error': 'Missing data'}

@app.get('/')
def get_all_json():
    auth_token = request.headers.get('Authorization')
    expected_token = os.getenv('API_KEY')

    if auth_token != f'Bearer {expected_token}':
        response.status = 401
        return {'error': 'Unauthorized'}

    result = {"trades": [], "wallets": []}
    for document in db['trades'].find():
        result["trades"].append(document['json_data'])
    for document in db['wallets'].find():
        result["wallets"].append(document['json_data'])

    db['trades'].delete_many({})
    db['wallets'].delete_many({})
    result = {
        'size':
        {
            "trades": len(result["trades"]),
            "wallets": len(result["wallets"])
        },
        'data': result
    }
    return result

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
