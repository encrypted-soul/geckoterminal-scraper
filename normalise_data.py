import json
import os

def process_file(file_path, normalized_pools, normalized_tokens, normalized_chains):
    with open(file_path, 'r') as f:
        data = json.load(f)

    pools = data['data']
    included = data['included']

    for pool in pools:
        pool_id = pool['id']
        attributes = pool['attributes']
        relationships = pool['relationships']

        if pool_id not in normalized_pools:
            dex_id = relationships['dex']['data']['id']
            dex = next(item for item in included if item['id'] == dex_id and item['type'] == 'dex')
            dex_name = dex['attributes']['name']

            network_id = dex['relationships']['network']['data']['id']
            network = next(item for item in included if item['id'] == network_id and item['type'] == 'network')
            network_name = network['attributes']['name']

            token_ids = [token['id'] for token in relationships['tokens']['data']]

            normalized_pools[pool_id] = {
                'id': pool_id,
                'name': attributes['name'],
                'address': attributes['address'],
                'dex': dex_name,
                'network': network_name,
                'token1_id': token_ids[0],
                'token2_id': token_ids[1],
                'reserve_in_usd': attributes['reserve_in_usd'],
                'base_token_id': attributes.get('base_token_id'),
                'swap_count_24h': attributes.get('swap_count_24h')
            }

    for item in included:
        if item['type'] == 'token':
            token_id = item['id']
            attributes = item['attributes']
            if token_id not in normalized_tokens:
                normalized_tokens[token_id] = {
                    'id': token_id,
                    'name': attributes['name'],
                    'symbol': attributes['symbol'],
                    'address': attributes['address']
                }
        elif item['type'] == 'network':
            chain_id = item['id']
            attributes = item['attributes']
            if chain_id not in normalized_chains:
                normalized_chains[chain_id] = {
                    'id': chain_id,
                    'name': attributes['name'],
                    'identifier': attributes['identifier'],
                    'chain_id': attributes['chain_id'],
                    'native_currency_symbol': attributes['native_currency_symbol'],
                    'native_currency_address': attributes['native_currency_address']
                }

def normalize_data(input_directory, pools_output_file, tokens_output_file, chains_output_file):
    normalized_pools = {}
    normalized_tokens = {}
    normalized_chains = {}

    for filename in os.listdir(input_directory):
        if filename.endswith('.json'):
            file_path = os.path.join(input_directory, filename)
            process_file(file_path, normalized_pools, normalized_tokens, normalized_chains)

    with open(pools_output_file, 'w') as f:
        json.dump(list(normalized_pools.values()), f, indent=2)

    with open(tokens_output_file, 'w') as f:
        json.dump(list(normalized_tokens.values()), f, indent=2)

    with open(chains_output_file, 'w') as f:
        json.dump(list(normalized_chains.values()), f, indent=2)

    print(f"Data normalized and saved to {pools_output_file}, {tokens_output_file}, and {chains_output_file}")
    print(f"Total unique pools: {len(normalized_pools)}")
    print(f"Total unique tokens: {len(normalized_tokens)}")
    print(f"Total unique chains: {len(normalized_chains)}")

# Usage
input_directory = 'trending_tokens'  # Directory containing all input JSON files
pools_output_file = 'pools.json'
tokens_output_file = 'tokens.json'
chains_output_file = 'chains.json'
normalize_data(input_directory, pools_output_file, tokens_output_file, chains_output_file)