import pymongo
from bson import ObjectId
from bottle import Bottle, run, response
from io import BytesIO

class MongoDBHandler:
    def __init__(self, uri: str, db_name: str):
        self.client = pymongo.MongoClient(uri)
        self.db = self.client[db_name]

    def get_zip_files_list(self):
        files = self.db['file_storage'].find({}, {"_id": 1, "file_name": 1, "file_type": 1, "file_size": 1})
        # Utiliser get() pour éviter KeyError
        return [
            {
                "_id": str(file["_id"]),
                "file_name": file.get("file_name", None),
                "file_type": file.get("file_type", None),
                "file_size": file.get("file_size", None)
            }
            for file in files
        ]

    def get_zip_file(self, file_id):
        file_data = self.db['file_storage'].find_one({"_id": ObjectId(file_id)})
        if file_data:
            return file_data['file_name'], file_data['file_data'], file_data['file_size']
        return None, None, None

app = Bottle()
mongo_handler = MongoDBHandler('mongodb://mongodb:27017/', 'json_db')

@app.route('/')
def list_zip_files():
    """
    Route pour lister tous les fichiers ZIP disponibles.
    """
    files = mongo_handler.get_zip_files_list()
    return {'files': files}

@app.route('/<file_id>')
def download_zip(file_id):
    """
    Route pour télécharger un fichier ZIP par son ID.
    :param file_id: ID du fichier dans MongoDB.
    """
    zip_name, zip_data, zip_size = mongo_handler.get_zip_file(file_id)
    if zip_data is not None:
        response.content_type = 'application/zip'
        response.headers['Content-Length'] = zip_size
        response.headers['Content-Disposition'] = f'attachment; filename="{zip_name}.zip"'
        return zip_data
    return {'error': 'File not found'}, 404

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
