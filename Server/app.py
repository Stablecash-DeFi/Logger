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
                data[i]["timestamp"] = dt
                collection.insert_one({'json_data': data[i]})
        else:
            data["timestamp"] = dt
            collection.insert_one({'json_data': data})
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
    return {'data': result}

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
