import http.client
import json
import time
import csv
import os
from pathlib import Path
from typing import Dict, Any, List

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
    def convert(json_data: List[Dict[str, Any]], csv_filename: str):
        """
        Convert JSON data into a CSV file.

        :param json_data: The JSON data to convert.
        :param csv_filename: The name of the file to save the CSV data to.
        """
        with open(csv_filename, 'w', newline='') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=json_data[0].keys())
            writer.writeheader()
            for record in json_data:
                writer.writerow(record)

def main():
    """
    Main function to orchestrate data fetching, processing, and saving.
    """
    url = os.getenv('DATA_FETCHER_URL', None)
    token = os.getenv('BEARER_TOKEN', None)

    if url is None or token is None:
        exit(1)

    headers = {
        'authorization': "Bearer {token}",
    }
    json_filename = '/app/data/data.json'
    max_records = 50000

    fetcher = DataFetcher(url, headers)
    json_manager = JSONFileManager(json_filename)

    data = fetcher.fetch()
    if data == {"error": "Unauthorized"}:
        print("error")
        exit(1)
    current_data = json_manager.load_data()
    current_data.append(data)

    if len(current_data) >= max_records:
        csv_filename = f"/app/data/data_{int(time.time())}.csv"
        CSVConverter.convert(current_data[:max_records], csv_filename)
        current_data = current_data[max_records:]
        if not current_data:
            current_data = []

    json_manager.save_data(current_data)
    time.sleep(300)

if __name__ == "__main__":
    main()
