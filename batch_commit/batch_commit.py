#!/usr/bin/python3

import os
import time
import requests
import configparser
import json
from datetime import datetime

# Load configuration
CONFIG_FILE_PATH = '/usr/local/bin/batch_commit.conf'


def read_config(config_file_path):
    config = configparser.ConfigParser()
    config.read(config_file_path)
    return config

def log_message(log_path, message):
    with open(log_path, 'a') as log_file:
        log_file.write(f"{datetime.now()} - {message}\n")

def fetch_data(base_url, query_type):
    query_map = {
        "basefee": "lotus_chain_basefee",
        "commits": "lotus_miner_sector_status"
    }
    
    url = f"{base_url}?query={query_map[query_type]}"
    response = requests.get(url)
    response.raise_for_status()  # To ensure any HTTP errors are raised
    return response.json()

def get_base_fee(data, lotus_instance):
    base_fee = next(item['value'][1] for item in data['data']['result'] if item['metric']['instance'] == lotus_instance)
    return int(base_fee)

def get_commits(data, miner_id):
    commits = next(item['value'][1] for item in data['data']['result'] if item['metric']['miner'] == miner_id and item['metric']['status'] == 'SCA')
    return int(commits)

def commit_sectors():
    os.system("lotus-miner sectors batching commit --publish-now")

def main():
    config = read_config(CONFIG_FILE_PATH)
    
    # Set the LOTUS_MINER_PATH environment variable
    lotus_miner_path = config['lotus']['MinerPath']
    os.environ['LOTUS_MINER_PATH'] = lotus_miner_path

    base_fee_threshold = int(config['lotus']['BasefeeThreshold'])
    commit_threshold = int(config['lotus']['CommitThreshold'])
    base_url = config['prometheus']['BaseURL']
    miner_id = config['lotus']['MinerID']
    lotus_instance = config['prometheus']['LotusInstance']
    log_path = config['log']['LogPath']

    log_message(log_path,"Starting...")
    log_message(log_path,f"Base fee threshold: {base_fee_threshold}")

    base_fee_data = fetch_data(base_url, "basefee")
    base_fee = get_base_fee(base_fee_data, lotus_instance)

    if base_fee < base_fee_threshold:
        commits_data = fetch_data(base_url, "commits")
        commits = get_commits(commits_data, miner_id)

        if commits > commit_threshold:
            log_message(log_path,f"Committing (base fee: {base_fee})")
            commit_sectors()
            log_message(log_path,"Committed - sleeping for 1m")
        else:
            log_message(log_path,f"Not enough commits ({commits}) - sleeping for 5 minutes")
    else:
        log_message(log_path,f"Base fee too high. BaseFee: {base_fee} - sleeping for 5 minutes")

if __name__ == "__main__":
    main()
