import pymongo
import csv
import zipfile
from io import BytesIO, StringIO
from datetime import datetime
from typing import Dict, List, Any, Union

class MongoDBHandler:
    """
    Classe pour gérer les interactions avec une base de données MongoDB.
    """

    def __init__(self, uri: str, db_name: str):
        self.client = pymongo.MongoClient(uri)
        self.db = self.client[db_name]

    def read_data(self, collection_name: str, limit: int) -> List[Dict[str, Any]]:
        return list(self.db[collection_name].find({}).sort('timestamp', 1).limit(limit))

    def delete_documents(self, collection_name: str, documents: List[Dict[str, Any]]) -> None:
        ids = [doc['_id'] for doc in documents]
        self.db[collection_name].delete_many({'_id': {'$in': ids}})

    def store_file(self, file_io: BytesIO, collection_name: str, file_type: str, file_name:str) -> None:
        self.db['file_storage'].insert_one({
            "collection_name": collection_name,
            "file_type": file_type,
            "file_data": file_io.getvalue(),
            "file_name": file_name,
            "file_size": file_io.getbuffer().nbytes
        })

class CSVExporter:
    """
    Classe pour exporter les données en CSV.
    """

    @staticmethod
    def flatten_json(nested_json: Dict[str, Any]) -> Dict[str, Any]:
        """
        Flatten a nested json object into a single level.

        :param nested_json: A dictionary representing a nested JSON object.
        :return: A flat dictionary where each key represents a path through the original nested structure.
        """
        out = {}

        def flatten(x: Union[Dict[str, Any], List[Any], Any], name: str = ''):
            """
            Recursive helper function that flattens the json.

            :param x: The current object being processed.
            :param name: Accumulated path string for the current object.
            """
            if isinstance(x, dict):
                for a in x:
                    flatten(x[a], name + a + '.')
            elif isinstance(x, list):
                i = 0
                for a in x:
                    flatten(a, name + str(i) + '.')
                    i += 1
            else:
                out[name[:-1]] = x

        flatten(nested_json)
        return out

    @staticmethod
    def export_to_csv_in_memory(data: List[Dict[str, Any]], filename: str) -> BytesIO:
        flat_data = [CSVExporter.flatten_json(record) for record in data]
        fieldnames = set()
        for record in flat_data:
            fieldnames.update(record.keys())
        csv_io = StringIO()
        writer = csv.DictWriter(csv_io, fieldnames=fieldnames)
        writer.writeheader()
        for record in flat_data:
            writer.writerow(record)
        csv_io.seek(0)
        return csv_io

class ZipCompressor:
    """
    Classe pour compresser des fichiers en ZIP.
    """

    @staticmethod
    def export_to_zip_in_memory(csv_files: Dict[str, BytesIO]) -> BytesIO:

        zip_io = BytesIO()
        with zipfile.ZipFile(zip_io, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for filename, file_io in csv_files.items():
                zipf.writestr(filename, file_io.getvalue())
        zip_io.seek(0)
        return zip_io

class DataExporter:
    """
    Classe pour gérer l'exportation des données de MongoDB vers des fichiers CSV et ZIP.
    """

    def __init__(self, mongo_handler: MongoDBHandler):
        self.mongo_handler = mongo_handler

    def export_data(self, export_limit: int = 10):
        if self.mongo_handler.db['trades'].count_documents({}) > export_limit:
            date_id = str(datetime.today().strftime('%Y-%m-%d'))
            date_month = str(datetime.today().strftime('%Y-%m'))
            trade_data = self.mongo_handler.read_data('trades', export_limit)
            wallet_data = self.mongo_handler.read_data('wallets', export_limit)

            trade_csv = CSVExporter.export_to_csv_in_memory(trade_data, 'trade.csv')
            wallet_csv = CSVExporter.export_to_csv_in_memory(wallet_data, 'wallet.csv')

            zip_file = ZipCompressor.export_to_zip_in_memory({'trade.csv': trade_csv, 'wallet.csv': wallet_csv})
            self.mongo_handler.store_file(zip_file, f"{date_month}",'zip', f"trades_{date_id}")

            self.mongo_handler.delete_documents('trades', trade_data)
            self.mongo_handler.delete_documents('wallets', wallet_data)

# Utilisation des classes
mongo_handler = MongoDBHandler('mongodb://mongodb:27017/', 'json_db')
data_exporter = DataExporter(mongo_handler)
data_exporter.export_data()
