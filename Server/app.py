from bottle import Bottle, request, response
import os

app = Bottle()

json_data = []

@app.post('/')
def receive_json():
    auth_token = request.headers.get('Authorization')
    expected_token = os.getenv('API_KEY')
    if auth_token != f'Bearer {expected_token}':
        response.status = 401
        return {'error': 'Unauthorized'}
    data = request.json
    if data:
        if isinstance(data, list):
            for i in range(len(data)):
                json_data.append(data[i])
        else:
            json_data.append(data)
        return {'message': None}
    else:
        return {'error': 'Missing data'}

@app.get('/')
def get_all_json():
    auth_token = request.headers.get('Authorization')
    expected_token = os.getenv('API_KEY')

    if auth_token != f'Bearer {expected_token}':
        response.status = 401
        return {'error': 'Unauthorized'}
    return {'json_data': json_data}

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
