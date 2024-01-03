import http.client
import json
import time
import csv
import os
from pathlib import Path
from typing import Dict, Any, List, Union
import zipfile

class DataFetcher:
    """
    Fetches data from a specified HTTP endpoint.
    """

    def __init__(self, url: str, headers: Dict[str, str]):
        """
        Initialize the DataFetcher.

        :param url: The URL to fetch data from.
        :param headers: The headers to use for the HTTP request.
        """
        self.url = url
        self.headers = headers

    def fetch(self) -> Dict[str, Any]:
        """
        Perform an HTTP GET request to fetch data.

        :return: The JSON-decoded response data.
        """
        conn = http.client.HTTPConnection(self.url)
        conn.request("GET", "/", headers=self.headers)
        response = conn.getresponse()
        data = response.read()
        return json.loads(data.decode("utf-8"))

class JSONFileManager:
    """
    Manages JSON file operations.
    """

    def __init__(self, filename: str):
        """
        Initialize the JSONFileManager.

        :param filename: The name of the file to manage.
        """
        self.filename = filename

    def load_data(self) -> List[Dict[str, Any]]:
        """
        Load data from the JSON file.

        :return: Data loaded from the file.
        """
        try:
            with open(self.filename, 'r') as file:
                return json.load(file)
        except FileNotFoundError:
            return []

    def save_data(self, data: List[Dict[str, Any]]):
        """
        Save data to the JSON file.

        :param data: The data to save to the file.
        """
        with open(self.filename, 'w') as file:
            json.dump(data, file)

class CSVConverter:
    """
    Converts JSON data to CSV format.
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
    def convert(json_data: List[Dict[str, Any]], csv_filename: str):
        """
        Convert JSON data into a CSV file.

        :param json_data: The JSON data to convert.
        :param csv_filename: The name of the file to save the CSV data to.
        """
        flat_data = [CSVConverter.flatten_json(record) for record in json_data]
        fieldnames = set()
        for record in flat_data:
            fieldnames.update(record.keys())
        with open(csv_filename, 'w', newline='') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for record in flat_data:
                writer.writerow(record)

def compact_csv_files(directory: str, prefix: str):
    """
    Compacts CSV files in a specified directory, combining ten files at a time.
    Handles CSV files with different fields by dynamically determining the complete set of fields.

    :param directory: The directory to search for CSV files.
    :param prefix: The prefix of the CSV files to combine.
    """
    csv_files = sorted([f for f in Path(directory).glob(f'{prefix}*.csv') if "combined" not in str(f)])

    while len(csv_files) >= 10:
        fieldnames = set()
        for file in csv_files[:10]:
            with open(file, 'r') as in_file:
                reader = csv.DictReader(in_file)
                fieldnames.update(reader.fieldnames)
        combined_csv = f"{directory}/{prefix}_{int(time.time())}_combined.csv"
        with open(combined_csv, 'w', newline='') as out_file:
            writer = csv.DictWriter(out_file, fieldnames=sorted(list(fieldnames)))
            writer.writeheader()
            for file in csv_files[:10]:
                with open(file, 'r') as in_file:
                    reader = csv.DictReader(in_file)
                    for row in reader:
                        row_with_all_fields = {field: row.get(field, None) for field in fieldnames}
                        writer.writerow(row_with_all_fields)
                file.unlink()
        csv_files = csv_files[10:]
        
def very_compact_csv_files(directory: str, prefix: str):
    """
    Compacts CSV files in a specified directory, combining ten files at a time.
    Handles CSV files with different fields by dynamically determining the complete set of fields.

    :param directory: The directory to search for CSV files.
    :param prefix: The prefix of the CSV files to combine.
    """
    csv_files = sorted([f for f in Path(directory).glob(f'{prefix}*.csv') if "combined" in str(f)])

    while len(csv_files) >= 10:
        fieldnames = set()
        for file in csv_files[:10]:
            with open(file, 'r') as in_file:
                reader = csv.DictReader(in_file)
                fieldnames.update(reader.fieldnames)
        name = f"{prefix}_{int(time.time())}_compact"
        combined_csv = f"{directory}/{name}.csv"
        with open(combined_csv, 'w', newline='') as out_file:
            writer = csv.DictWriter(out_file, fieldnames=sorted(list(fieldnames)))
            writer.writeheader()
            for file in csv_files[:10]:
                with open(file, 'r') as in_file:
                    reader = csv.DictReader(in_file)
                    for row in reader:
                        row_with_all_fields = {field: row.get(field, None) for field in fieldnames}
                        writer.writerow(row_with_all_fields)
                file.unlink()
        csv_files = csv_files[10:]
        zip_file = f"{name}.zip"
        zip_path = f"{directory}/{zip_file}"
        with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(csv_file, arcname=f'{name}.csv')

def main():
    """
    Main function to orchestrate data fetching, processing, and saving.
    """
    url = os.getenv('DATA_FETCHER_URL', None)
    token = os.getenv('BEARER_TOKEN', None)

    if url is None or token is None:
        exit(1)

    headers = {
        'authorization': f"Bearer {token}",
    }
    json_filename_trade = '/app/data/trades.json'
    json_filename_wallet = '/app/data/wallets.json'
    max_records = 10000

    fetcher = DataFetcher(url, headers)

    data = fetcher.fetch()
    if data == {"error": "Unauthorized"}:
        exit(1)

    json_manager = JSONFileManager(json_filename_trade)
    current_data = json_manager.load_data()
    current_data += data["data"]["trades"]
    stored = False
    print(len(current_data))
    while len(current_data) >= max_records:
        stored = True
        csv_filename = f"/app/data/trades_{int(time.time())}.csv"
        CSVConverter.convert(current_data[:max_records], csv_filename)
        current_data = current_data[max_records:]
        print(len(current_data), csv_filename)
        if not current_data:
            current_data = []
        json_manager.save_data(current_data)
        time.sleep(1)
        compact_csv_files('/app/data', 'trades')
        very_compact_csv_files('/app/data', 'trades')
    if stored is False:
        json_manager.save_data(current_data)

    json_manager = JSONFileManager(json_filename_wallet)
    current_data = json_manager.load_data()
    current_data += data["data"]["wallets"]
    stored = False
    while len(current_data) >= max_records:
        stored = True
        csv_filename = f"/app/data/wallets_{int(time.time())}.csv"
        CSVConverter.convert(current_data[:max_records], csv_filename)
        current_data = current_data[max_records:]
        if not current_data:
            current_data = []
        json_manager.save_data(current_data)
        time.sleep(1)
        compact_csv_files('/app/data', 'wallets')
        very_compact_csv_files('/app/data', 'wallets')
    if stored is False:
        json_manager.save_data(current_data)

    time.sleep(30)

if __name__ == "__main__":
    main()
