import bottle
bottle.BaseRequest.MEMFILE_MAX =  1024 * 1024

from bottle import Bottle, response, request, HTTPResponse, run
import os
import json
import hashlib
from pymongo import MongoClient
from datetime import datetime, timezone
from typing import Dict, List, Union, Any



class CurrencyConverter:
    """
    Handles currency conversion based on market rates.
    This class is responsible for converting amounts from one currency to another
    using the provided market rates (fiat prices).
    """

    def __init__(self, fiat_prices: Dict[str, float]):
        """
        Initializes the converter with market rates.
        :param fiat_prices: A dictionary of currency conversion rates.
        """
        self.fiat_prices = fiat_prices

    def convert(self, amount: float, from_currency: str, to_currency: str) -> float:
        """
        Converts an amount from one currency to another.
        Uses the fiat prices provided at initialization to perform the conversion.
        :param amount: The amount to convert.
        :param from_currency: The currency code to convert from.
        :param to_currency: The currency code to convert to.
        :return: The converted amount in the target currency.
        """
        if from_currency == to_currency:
            return amount
        if from_currency == "EUR":
            return amount * float(self.fiat_prices[to_currency])
        if to_currency == "EUR":
            return amount / float(self.fiat_prices[from_currency])
        return amount

class SwapCalculator:
    """
    Calculates and evaluates currency swaps.
    This class provides functionality to calculate the profitability and other metrics
    of a currency swap based on given configurations and market rates.
    """

    def __init__(self, swap_config: Dict[str, Union[str, float]], converter: CurrencyConverter):
        """
        Initializes the swap calculator with configuration and a currency converter.
        :param swap_config: Configuration for the swap including exchange rates and transaction costs.
        :param converter: A CurrencyConverter object for handling currency conversions.
        """
        self.swap_config = swap_config
        self.converter = converter

    def calculate_percentage(self, pairs: Dict[str, Dict[str, Union[str, float]]]) -> float:
        """
        Calculates the percentage gain or loss for a given currency pair.
        :param pairs: Dictionary containing the 'from' and 'to' currency information.
        :return: The percentage gain or loss for the swap.
        """
        flat_gain = 100 * self.swap_config['exchangeRate']
        poundered_amount = self._poundered_amount(pairs)
        return flat_gain - poundered_amount

    def _poundered_amount(self, pairs: Dict[str, Dict[str, Union[str, float]]]) -> float:
        """
        Calculates the poundered amount for a given currency pair.
        :param pairs: Dictionary containing the 'from' and 'to' currency information.
        :return: The poundered amount after conversion.
        """
        from_currency = pairs['from']['currency']
        to_currency = pairs['to']['currency']
        return self.converter.convert(100, from_currency, to_currency) if self._is_relevant_pair(from_currency, to_currency) else 100

    def _is_relevant_pair(self, from_currency: str, to_currency: str) -> bool:
        """
        Checks if the currency pair is relevant for conversion.
        :param from_currency: The source currency code.
        :param to_currency: The target currency code.
        :return: True if the pair is relevant, False otherwise.
        """
        return [from_currency, to_currency] in [["EUR", "USD"], ["USD", "EUR"]]

class TradeDataProcessor:
    """
    Processes trade data for database insertion.
    This class contains methods to handle different aspects of trade data.
    """

    def __init__(self):
        self.converter = None
        self.timestamp = int(datetime.now().replace(tzinfo=timezone.utc).timestamp())

    def wallet_value(self, wallet: Dict[str, float]) -> Dict[str, Union[Dict[str, float], float]]:
        """
        Calculates the total value of the wallet by chain and currency.
        Sums up the values in EUR and USD, then converts EUR to USD for the total value.
        :param wallet: A dictionary representing the wallet with chain and currency as keys.
        :return: A dictionary containing the calculated wallet values.
        """
        ret = {"by_chain": {}, "total": 0.0}
        for chain in ["137", "solana"]:
            chain_data = self._calculate_chain_values(wallet, chain)
            ret["by_chain"][chain] = chain_data
            ret["total"] += chain_data["total"]
        ret["total"] = float(f'{ret["total"]:.6f}')
        return ret

    def _calculate_chain_values(self, wallet: Dict[str, float], chain: str) -> Dict[str, float]:
        """
        Calculates the values for a specific chain within the wallet.
        :param wallet: A dictionary representing the wallet.
        :param chain: The specific chain (e.g., "137", "solana") to calculate values for.
        :return: A dictionary containing the values for the specified chain.
        """
        eur_sum = sum(wallet[i] for i in wallet if i.split(":")[0] == chain and i[-3:] == 'EUR')
        usd_sum = sum(wallet[i] for i in wallet if i.split(":")[0] == chain and i[-3:] == 'USD')
        total = usd_sum + self.converter.convert(eur_sum, "EUR", "USD")
        ret_data = {"EUR": eur_sum, "USD": usd_sum, "total": total}
        return {i:float(f'{ret_data[i]:.6f}') for i in ret_data}

    def process_trade_config(self, swap_config: Dict[str, Any]) -> None:
        """
        Processes the swap configuration part of the trade data.
        Modifies the swap_config dictionary in place, adjusting amounts and costs.
        :param swap_config: A dictionary containing the swap configuration data.
        """
        swap_config["gasCosts"] = [float(f'{float(cost["amountUsd"]):.6f}') for cost in swap_config["gasCosts"]]
        swap_config["feeCosts"] = [float(f'{float(cost["amountUsd"]):.6f}') for cost in swap_config["feeCosts"]]

    def process_trade_pair(self, trade_pair: Dict[str, Dict[str, str]]) -> None:
        """
        Processes the trade pair part of the trade data.
        Modifies the trade_pair dictionary in place, formatting the 'from' and 'to' fields.
        :param trade_pair: A dictionary containing the 'from' and 'to' currency information.
        """
        trade_pair["from"] = f'{trade_pair["from"]["chain"]}:{trade_pair["from"]["token"]}:{trade_pair["from"]["currency"]}'
        trade_pair["to"] = f'{trade_pair["to"]["chain"]}:{trade_pair["to"]["token"]}:{trade_pair["to"]["currency"]}'

    def process_wallet_data(self, wallet_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Processes the wallet data part of the trade data.
        Returns a dictionary with formatted wallet data.
        :param wallet_data: A list of dictionaries, each representing a wallet entry.
        :return: A dictionary with processed wallet data.
        """
        processed_data = self._process_entries(wallet_data)
        ret_data = self._construct_wallet_return_data(processed_data)
        return ret_data

    def _process_entries(self, wallet_data: List[Dict[str, Any]]) -> Dict[str, float]:
        """
        Processes each entry in the wallet data.
        :param wallet_data: A list of dictionaries, each representing a wallet entry.
        :return: A dictionary with processed wallet entries.
        """
        processed_data = {}
        for entry in wallet_data:
            key = self._generate_wallet_key(entry)
            processed_data[key] = float(f'{entry["amount"]:.6f}')
        return processed_data

    def _generate_wallet_key(self, entry: Dict[str, Any]) -> str:
        """
        Generates a unique key for a wallet entry.
        :param entry: A dictionary representing a wallet entry.
        :return: A string key for the wallet entry.
        """
        return f'{entry["chain"]}:{entry["address"]}:{entry["currency"]}'

    def _construct_wallet_return_data(self, wallet_data: Dict[str, float]) -> Dict[str, Any]:
        """
        Constructs the final return data structure for wallet processing.
        :param wallet_data: A dictionary with processed wallet data.
        :return: A dictionary representing the structured wallet return data.
        """
        ret_data = {
            "data": {
                "stable_coins": wallet_data,
                "value": self.wallet_value(wallet_data),
            },
            "timestamp": self.timestamp
        }
        ret_data["_id"] = hashlib.md5(str(ret_data["data"]).encode()).hexdigest()
        return ret_data

    def format_trade_document(self, trade_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Formats the final trade document for database insertion.
        This function restructures and prepares the entire trade_data dictionary.
        :param trade_data: A dictionary containing the processed trade data.
        :return: Formatted trade document.
        """
        self.converter = CurrencyConverter(trade_data["trade"]["swapConfig"]["fiatPrices"])
        self.SwapCalculator = SwapCalculator(trade_data["trade"]["swapConfig"], self.converter)
        renta = self.SwapCalculator.calculate_percentage(trade_data["trade"]["pair"])
        renta = float(f'{renta:.6f}')
        self.process_trade_config(trade_data["trade"]["swapConfig"])
        self.process_trade_pair(trade_data["trade"]["pair"])
        trade_data["trade"] = self.process_trade_all(trade_data["trade"], renta)
        trade_data["wallet"] = self.process_wallet_data(trade_data["wallet"])
        return trade_data

    def process_trade_all(self, trade_data: Dict[str, Any], renta: float) -> Dict[str, Any]:
        """
        Processes all trade-related data and consolidates it into a structured format.
        This method organizes various aspects of the trade, including costs, exchange rates,
        and prices in different currencies, along with the calculated rentability.

        :param trade_data: A dictionary containing the data related to the trade.
        :param renta: The calculated rentability of the trade.
        :return: A dictionary with structured trade information.
        """
        return {
                    "cost": {
                        "gas": trade_data["swapConfig"]["gasCosts"],
                        "fee": trade_data["swapConfig"]["feeCosts"],
                        "total": trade_data["swapConfig"]["transactionCost"]
                    },
                    "exchange": {
                        "rate":  float(f'{trade_data["swapConfig"]["exchangeRate"]:.6f}'),
                        "from": trade_data["pair"]["from"],
                        "to": trade_data["pair"]["to"]
                    },
                    "price": {
                        "USD": 1.00,
                        "EUR": trade_data["swapConfig"]["fiatPrices"]["USD"],
                        "SOL": trade_data["solanaPrice"],
                        "MAT": trade_data["maticPrice"]
                    },
                    "rentability": renta,
                    "timestamp": self.timestamp
                }

class AuthorizationValidator:
    """
    Validates authorization tokens for incoming requests.
    """

    def __init__(self, expected_token: str):
        """
        Initializes the validator with the expected token.
        :param expected_token: The expected authorization token for validation.
        """
        self.expected_token = expected_token

    def validate(self, token: str) -> bool:
        """
        Validates the provided authorization token.
        :param token: The authorization token to be validated.
        :return: True if the token is valid, False otherwise.
        """
        return token == f'Bearer {self.expected_token}'

class RequestValidator:
    """
    Validates the request data for API calls.
    This class contains static methods to ensure that the data received in API requests
    is valid and conforms to the expected format and types.
    """

    @staticmethod
    def validate_trade_data(data: Dict[str, Any]) -> (bool, str):
        if not isinstance(data, dict) or "swapConfig" not in data or "pair" not in data:
            return False, "Trade data must be a dictionary with 'swapConfig' and 'pair'."

        swap_config = data["swapConfig"]
        if not all(key in swap_config for key in ["fromAmount", "toAmount", "exchangeRate", "transactionCost", "fiatPrices"]):
            return False, "Missing keys in 'swapConfig'."
        if not isinstance(swap_config["fiatPrices"], dict):
            return False, "'fiatPrices' in 'swapConfig' should be a dictionary."

        pair = data["pair"]
        for side in ["from", "to"]:
            if side not in pair or not isinstance(pair[side], dict):
                return False, f"Missing or invalid '{side}' in 'pair'."
            if not all(key in pair[side] for key in ["type", "chain", "token", "address", "currency"]):
                return False, f"Missing keys in 'pair.{side}'."

        return True, ""

    @staticmethod
    def validate_wallet_data(wallet: List[Dict[str, Any]]) -> (bool, str):
        """
        Validates the structure and type of wallet data.
        :param wallet: The wallet data to validate.
        :return: Tuple of (bool, str) indicating if the data is valid and an error message if not.
        """
        if not isinstance(wallet, list):
            return False, "Wallet data should be a list."

        for item in wallet:
            if not isinstance(item, dict):
                return False, "Each item in wallet data should be a dictionary."

            if "address" not in item or not isinstance(item["address"], str):
                return False, "Wallet item 'address' is missing or not a string."
            if "amount" not in item or not isinstance(item["amount"], (float, str, int)):
                return False, "Wallet item 'amount' is missing or not a float or string."
            if isinstance(item["amount"], str):
                try:
                    item["amount"] = float(item["amount"])
                except ValueError:
                    return False, "Wallet item 'amount' string could not be converted to float."
            if "chain" not in item or not isinstance(item["chain"], str):
                return False, "Wallet item 'chain' is missing or not a string."
            if "type" not in item or not isinstance(item["type"], str):
                return False, "Wallet item 'type' is missing or not a string."
            if "currency" not in item or not isinstance(item["currency"], str):
                return False, "Wallet item 'currency' is missing or not a string."

        return True, ""

class MongoDBHandler:
    """
    Handles interactions with a MongoDB database.
    This class provides methods to perform database operations like inserting and finding documents.
    """

    def __init__(self, uri: str, db_name: str):
        """
        Initializes the MongoDB connection.
        :param uri: The MongoDB URI string.
        :param db_name: The name of the database to use.
        """
        self.client = MongoClient(uri)
        self.db = self.client[db_name]

    def close_connection(self):
        """
        Closes the MongoDB connection.
        """
        self.client.close()

    def insert_trade(self, trade_data: Dict[str, Any]) -> None:
        """
        Inserts a trade document into the 'trades' collection.
        :param trade_data: The trade data to insert.
        """
        self.db['trades'].insert_one(trade_data)

    def insert_wallet(self, wallet_data: Dict[str, Any]) -> None:
        """
        Inserts a wallet document into the 'wallets' collection if it doesn't already exist.
        :param wallet_data: The wallet data to insert.
        """
        if not self.db['wallets'].find_one({"_id": wallet_data["_id"]}):
            self.db['wallets'].insert_one(wallet_data)

app = Bottle()
MONGO = MongoDBHandler('mongodb://mongodb:27017/', 'json_db')

@app.route('/health', method='GET')
def health_check():
    """
    Health check route to ensure the API is running.
    This route can be used for monitoring and health checks of the API service.
    :return: A simple message indicating the API is operational.
    """
    return json.dumps({"status": "healthy"})

# Rest of the server code
@app.post('/')
def receive_json():
    """
    Endpoint to receive and process JSON data from POST requests.
    This function validates the authorization token and processes the incoming JSON data.
    """
    validator = AuthorizationValidator(os.getenv('API_KEY'))
    auth_token = request.headers.get('Authorization')

    if not validator.validate(auth_token):
        response.status = 401
        return {'error': 'Unauthorized'}

    datas = request.json
    if not datas:
        return {'error': 'Missing data'}
    for data in datas:
        is_valid_trade, trade_error = RequestValidator.validate_trade_data(data.get("trade", {}))
        if not is_valid_trade:
            return HTTPResponse(status=400, body=json.dumps({"error": trade_error}))
        is_valid_wallet, wallet_error = RequestValidator.validate_wallet_data(data.get("wallet", []))
        if not is_valid_wallet:
            return HTTPResponse(status=400, body=json.dumps({"error": wallet_error}))
    processor = TradeDataProcessor()
    processed_data = [processor.format_trade_document(trade) for trade in datas]
    for index in range(len(processed_data)):
        MONGO.insert_trade(processed_data[index]["trade"])
        MONGO.insert_wallet(processed_data[index]["wallet"])
    return {'error': None}

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)
