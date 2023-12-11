import bottle
bottle.BaseRequest.MEMFILE_MAX =  1024 * 1024

from bottle import Bottle, request, response
import os
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

def rentability_percentage(pairs, fiat_prices, rate):
    plus = 100 * rate
    swap_gain = plus - 100
    if (pairs['from']['currency'] == 'EUR' and pairs['to']['currency'] != 'EUR') or (pairs['to']['currency'] == 'EUR' and pairs['from']['currency'] != 'EUR'):
        exchangeRate = plus
        marketRate = convert_market_rate(100, pairs['from']['currency'], pairs['to']['currency'], fiat_prices)
        corelleration_swap_to_market_benefices = exchangeRate - marketRate
        swap_gain = convert_market_rate(corelleration_swap_to_market_benefices, pairs['to']['currency'], 'USD', fiat_prices)
    if pairs['from']['currency'] == 'EUR' and pairs['to']['currency'] == 'EUR':
        swap_gain = convert_market_rate(swap_gain, pairs['from']['currency'], 'USD', fiat_prices)
    return swap_gain

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

                data[i]["trade"]["pair"]["from"] = {
                    "wallet_id": f'{data[i]["trade"]["pair"]["from"]["chain"]}:{data[i]["trade"]["pair"]["from"]["token"]}',
                    "currency": data[i]["trade"]["pair"]["from"]["currency"]
                }
                data[i]["trade"]["pair"]["to"] = {
                    "wallet_id": f'{data[i]["trade"]["pair"]["to"]["chain"]}:{data[i]["trade"]["pair"]["to"]["token"]}',
                    "currency": data[i]["trade"]["pair"]["to"]["currency"]
                }

                REF = {}
                for j in range(len(data[i]["wallet"])):
                    REF[f'{data[i]["wallet"][j]["chain"]}:{data[i]["wallet"][j]["address"]}'] = data[i]["wallet"][j]["amount"]
                data[i]["wallet"] = REF

                collection.insert_one({
                    "json_data": {
                        "trade": {
                            "cost": {
                                "gas": data[i]["trade"]["swapConfig"]["gasCosts"],
                                "fee": data[i]["trade"]["swapConfig"]["feeCosts"],
                                "total": data[i]["trade"]["swapConfig"]["transactionCost"]
                            },
                            "exchange": {
                                "rate":  data[i]["trade"]["swapConfig"]["exchangeRate"],
                                "from": data[i]["trade"]["pair"]["from"],
                                "to": data[i]["trade"]["pair"]["to"]
                            },
                            "rentability": rentability_percentage
                        },
                        "price": {
                            "USD": 1.00,
                            "EUR": data[i]["trade"]["swapConfig"]["fiatPrices"]["USD"],
                            "SOL": data[i]["trade"]["solanaPrice"],
                            "MAT": data[i]["trade"]["maticPrice"]
                        },
                        "wallet": data[i]["wallet"],
                        "timestamp": dt
                    }
                })
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
