import json
import requests
import hashlib
from bs4 import BeautifulSoup
import time
import concurrent.futures
from datetime import datetime, timedelta
import random
import logging
import threading

current_time = datetime.utcnow()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TokenTradeScraper:
    def __init__(self, pools_file, tokens_file, chains_file):
        self.pools = self.load_json(pools_file)
        self.tokens = self.load_json(tokens_file)
        self.chains = self.load_json(chains_file)
        self.base_url = "https://app.geckoterminal.com/api/p1/{}/pools/"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        self.symbol_to_ids = self.create_symbol_to_ids_map()
        self.request_count = 0
        self.last_request_time = time.time()
        self.request_lock = threading.Lock()

        # Arkham Intelligence API configuration
        self.arkham_base_url = "https://api.arkhamintelligence.com/balances/address/"
        self.arkham_api_host = "https://api.arkhamintelligence.com"
        self.arkham_client_key = "gh67j345kl6hj5k432"  # Replace with your actual client key

    def load_json(self, file_path):
        with open(file_path, "r") as f:
            return json.load(f)

    def create_symbol_to_ids_map(self):
        symbol_to_ids = {}
        for token in self.tokens:
            symbol = token["symbol"].lower()
            if symbol not in symbol_to_ids:
                symbol_to_ids[symbol] = []
            symbol_to_ids[symbol].append(token["id"])
        return symbol_to_ids

    def get_token_ids(self, symbol):
        return self.symbol_to_ids.get(symbol.lower(), [])

    def find_pools(self, input_token_ids, output_token_ids):
        matching_pools = []
        for pool in self.pools:
            if (pool["token1_id"] in input_token_ids and pool["token2_id"] in output_token_ids):
                matching_pools.append(pool)
        return matching_pools

    def get_chain_identifier(self, network_name):
        for chain in self.chains:
            if chain["name"] == network_name:
                return chain["identifier"]
        return None

    def normalize_trade(self, trade, input_token_ids, output_token_ids, pool_address, chain):
        attributes = trade["attributes"]
        relationships = trade["relationships"]

        from_token_id = relationships["from_token"]["data"]["id"]
        to_token_id = relationships["to_token"]["data"]["id"]

        is_input_to_output = (from_token_id in input_token_ids and to_token_id in output_token_ids)

        return {
            "timestamp": attributes["timestamp"],
            "tx_hash": attributes["tx_hash"],
            "trader_address": attributes["tx_from_address"],
            "input_amount": attributes["from_token_amount"] if is_input_to_output else attributes["to_token_amount"],
            "output_amount": attributes["to_token_amount"] if is_input_to_output else attributes["from_token_amount"],
            "input_token": from_token_id if is_input_to_output else to_token_id,
            "output_token": to_token_id if is_input_to_output else from_token_id,
            "price_in_usd": attributes["price_from_in_usd"] if is_input_to_output else attributes["price_to_in_usd"],
            "pool_address": pool_address,
            "chain": chain
        }

    def rate_limit_request(self):
        with self.request_lock:
            self.request_count += 1
            if self.request_count >= 5:  # Reset after every 5 requests
                current_time = time.time()
                time_passed = current_time - self.last_request_time
                if time_passed < 5:  # Ensure at least 5 seconds have passed
                    time.sleep(5 - time_passed)
                self.request_count = 0
                self.last_request_time = time.time()

    def make_request_with_retries(self, url, headers, params=None, max_retries=8, initial_delay=2):
        for attempt in range(max_retries):
            self.rate_limit_request()
            try:
                response = requests.get(url, headers=headers, params=params)
                if response.status_code == 200:
                    return response
                elif response.status_code == 429:
                    delay = initial_delay * (2 ** attempt) + random.uniform(0, 1)
                    logger.warning(f"Rate limit hit. Retrying in {delay:.2f} seconds...")
                    time.sleep(delay)
                else:
                    response.raise_for_status()
            except requests.RequestException as e:
                logger.error(f"Request failed: {e}")
                if attempt == max_retries - 1:
                    raise

        raise Exception("Max retries reached. Unable to complete request.")

    def fetch_trades(self, pool, input_token_ids, output_token_ids, max_trades=1000):
        chain_identifier = self.get_chain_identifier(pool["network"])
        if not chain_identifier:
            logger.error(f"Chain identifier not found for network: {pool['network']}")
            return []

        url = f"{self.base_url.format(chain_identifier)}{pool['address']}/swaps"
        trades = []

        # Create the start and end time for the range
        start_time = current_time - timedelta(minutes=1)
        end_time = current_time

        # Format the dates in the required ISO 8601 format
        start_time_str = start_time.strftime("%Y-%m-%dT%H:%M:%S.%f") + "+00:00"
        end_time_str = end_time.strftime("%Y-%m-%dT%H:%M:%S.%f") + "+00:00"

        # Create the date range string
        date_range = f"{start_time_str}_{end_time_str}"

        params = {
            "include": "from_token,to_token",
            "inverted": "0",
            "page[after]": date_range,
            "pair_id": pool["id"],
        }

        while len(trades) < max_trades:
            try:
                response = self.make_request_with_retries(url, headers=self.headers, params=params)
                data = response.json()
                new_trades = data.get("data", [])
                if not new_trades:
                    break

                for trade in new_trades:
                    normalized_trade = self.normalize_trade(trade, input_token_ids, output_token_ids, pool["address"], pool["network"])
                    trades.append(normalized_trade)

                if len(trades) >= max_trades:
                    break

                next_link = data.get("links", {}).get("next")
                if not next_link:
                    break

                url = f"https://app.geckoterminal.com{next_link}"
                params = {}

            except Exception as e:
                logger.error(f"Error fetching trades: {e}")
                break

        return trades

    def generate_x_payload(self, url, timestamp):
        """Generate the X-Payload using SHA-256 hashes."""
        path = url.replace(self.arkham_api_host, "")
        first_hash = hashlib.sha256(f"{path}:{timestamp}:{self.arkham_client_key}".encode()).hexdigest()
        x_payload = hashlib.sha256(f"{self.arkham_client_key}:{first_hash}".encode()).hexdigest()
        return x_payload

    def fetch_balance(self, wallet_address):
        """Fetch balance data for a given wallet address."""
        timestamp = str(int(time.time()))
        url = self.arkham_base_url + wallet_address
        x_payload = self.generate_x_payload(url, timestamp)

        headers = {
            "X-Payload": x_payload,
            "X-Timestamp": timestamp
        }

        try:
            logger.info(f"Fetching balance for address: {wallet_address}")
            response = self.make_request_with_retries(url, headers=headers)
            logger.info(f"Successfully fetched balance for address: {wallet_address}")
            return response.json()
        except Exception as e:
            logger.error(f"Failed to fetch balance for address: {wallet_address}. Error: {e}")
            return None

    def fetch_balances_for_trades(self, trades, max_workers=10):
        """Fetch balances for all unique addresses in the trades using multithreading."""
        unique_addresses = set(trade['trader_address'] for trade in trades)
        address_balances = {}

        logger.info(f"Fetching balances for {len(unique_addresses)} unique addresses")

        def fetch_balance_thread(address):
            balance_data = self.fetch_balance(address)
            if balance_data:
                return address, balance_data
            return None

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_address = {executor.submit(fetch_balance_thread, address): address for address in unique_addresses}
            for future in concurrent.futures.as_completed(future_to_address):
                result = future.result()
                if result:
                    address, balance_data = result
                    address_balances[address] = balance_data

        logger.info(f"Completed fetching balances. Retrieved {len(address_balances)} out of {len(unique_addresses)} addresses")
        return address_balances

    def scrape_trades_parallel(self, input_token_symbol, output_token_symbol, max_trades=1000, max_workers=5):
        input_token_ids = self.get_token_ids(input_token_symbol)
        output_token_ids = self.get_token_ids(output_token_symbol)

        if not input_token_ids or not output_token_ids:
            logger.error(f"Token symbols not found: {input_token_symbol} or {output_token_symbol}")
            return []

        matching_pools = self.find_pools(input_token_ids, output_token_ids)
        all_trades = []

        def fetch_trades_for_pool(pool):
            logger.info(f"Fetching trades for pool: {pool['address']} on {pool['network']}")
            trades = self.fetch_trades(pool, input_token_ids, output_token_ids, max_trades)
            logger.info(f"Fetched {len(trades)} trades from pool: {pool['address']}")
            return trades

        logger.info(f"The total number of matching pools is: {len(matching_pools)}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_pool = {executor.submit(fetch_trades_for_pool, pool): pool for pool in matching_pools}
            for future in concurrent.futures.as_completed(future_to_pool):
                pool = future_to_pool[future]
                try:
                    trades = future.result()
                    all_trades.extend(trades)
                except Exception as exc:
                    logger.error(f"Pool {pool['address']} generated an exception: {exc}")

        # Sort all trades by timestamp and take the last max_trades
        all_trades.sort(key=lambda x: x['timestamp'], reverse=True)
        last_1000_trades = all_trades[:max_trades]

        # Fetch balances for addresses in the last 1000 trades
        address_balances = self.fetch_balances_for_trades(last_1000_trades)

        # Add balance information to each trade
        for trade in last_1000_trades:
            trade['trader_balance'] = address_balances.get(trade['trader_address'], {})

        return last_1000_trades

# Usage
scraper = TokenTradeScraper("pools.json", "tokens.json", "chains.json")
input_token_symbol = "GIFF"
output_token_symbol = "WPLS"
trades = scraper.scrape_trades_parallel(input_token_symbol, output_token_symbol, max_trades=1000)

logger.info(f"Total trades fetched: {len(trades)}")
logger.info("Sample trade with balance:")
logger.info(json.dumps(trades[0], indent=2) if trades else "No trades found")

# Optionally, save all trades to a file
with open("trades_with_balances.json", "w") as f:
    json.dump(trades, f, indent=2)