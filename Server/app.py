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
collection = db['json_collection']

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
            "EUR": sum([wallet[i] for i in wallet if i[-3:] == 'EUR']),
            "USD": sum([wallet[i] for i in wallet if i[-3:] == 'USD']),
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
                data[i]["trade"]["swapConfig"]["gasCosts"] = [float(cost["amountUsd"]) for cost in data[i]["trade"]["swapConfig"]["gasCosts"]]
                data[i]["trade"]["swapConfig"]["feeCosts"] = [float(cost["amountUsd"]) for cost in data[i]["trade"]["swapConfig"]["feeCosts"]]

                data[i]["trade"]["pair"]["from"] = f'{data[i]["trade"]["pair"]["from"]["chain"]}:{data[i]["trade"]["pair"]["from"]["token"]}:{data[i]["trade"]["pair"]["from"]["currency"]}'
                data[i]["trade"]["pair"]["to"] = f'{data[i]["trade"]["pair"]["to"]["chain"]}:{data[i]["trade"]["pair"]["to"]["token"]}:{data[i]["trade"]["pair"]["to"]["currency"]}'

                REF = {}
                for j in range(len(data[i]["wallet"])):
                    index = f'{data[i]["wallet"][j]["chain"]}:{data[i]["wallet"][j]["address"]}:{data[i]["wallet"][j]["currency"]}'
                    REF[index] = float(f'{data[i]["wallet"][j]["amount"]:.6f}')
                data[i]["wallet"] = REF

                d = {
                    "trade": {
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
                        "rentability": None,
                    },
                    "price": {
                        "USD": 1.00,
                        "EUR": data[i]["trade"]["swapConfig"]["fiatPrices"]["USD"],
                        "SOL": data[i]["trade"]["solanaPrice"],
                        "MAT": data[i]["trade"]["maticPrice"]
                    },
                    "wallet": {
                        "id": None,
                        "data": {
                            "stable_coins": data[i]["wallet"],
                            "value": None,
                        },
                        "timestamp": dt
                    },
                    "timestamp": dt
                }
                d["wallet"]["data"]["value"] = wallet_value(d["wallet"]["data"]["stable_coins"], d["price"])
                d["wallet"]["id"] = hashlib.sha256(str(d["wallet"]["data"]).encode()).hexdigest()
                renta = rentability(
                    amount = 100,
                    from_currency = d["trade"]["exchange"]["from"]["currency"],
                    to_currency = d["trade"]["exchange"]["to"]["currency"],
                    price = d["price"],
                    rate = d["trade"]["exchange"]["rate"]
                )
                d["trade"]["rentability"] = float(f"{renta:.6f}")
                collection.insert_one({"json_data": d})
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

    result = []
    for document in collection.find():
        result.append(document['json_data'])
    collection.delete_many({})
    return {'size': len(result), 'data': result}

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
