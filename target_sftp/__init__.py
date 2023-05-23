#!/usr/bin/env python3
import os
import json
import argparse
import logging

from target_sftp import client

logger = logging.getLogger("target-sftp")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def load_json(path):
    with open(path) as f:
        return json.load(f)


def parse_args():
    '''Parse standard command-line args.
    Parses the command-line arguments mentioned in the SPEC and the
    BEST_PRACTICES documents:
    -c,--config     Config file
    Returns the parsed args object from argparse. For each argument that
    point to JSON files (config, state, properties), we will automatically
    load and parse the JSON file.
    '''
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-c', '--config',
        help='Config file',
        required=True)

    args = parser.parse_args()
    if args.config:
        setattr(args, 'config_path', args.config)
        args.config = load_json(args.config)

    return args

def make_or_change_directory(sftp_client,dir,create_and_move=False):
    try: 
        sftp_client.chdir(dir)
    except IOError:
        sftp_client.mkdir(dir)
        if create_and_move:
            logger.info(f"Creating folder {dir} at {sftp_client.cwd()}")
            sftp_client.chdir(dir)


def upload(args):
    logger.info(f"Exporting data...")
    config = args.config

    # Upload all data in input_path to sftp
    sftp_conection = client.connection(config)
    sftp_client = sftp_conection.sftp

    make_or_change_directory(sftp_client,config["path_prefix"],True)


    for root, dirs, files in os.walk(config["input_path"]):
        head, cwd = os.path.split(root)
        if cwd:
            sftp_client.chdir(cwd)
        for dir in dirs:
            make_or_change_directory(sftp_client, dir)
        
        for file in files:
            file_path = os.path.join(root, file)
            logger.info(f"Uploading {file} to {config['path_prefix']} at {sftp_client.cwd()}")
            sftp_client.put(file_path, file)
        
        if cwd:
            sftp_client.chdir("..")
        
        
        
    sftp_conection.close()
    logger.info(f"Data exported.")


def main():
    # Parse command line arguments
    args = parse_args()

    # Upload the data
    upload(args)


if __name__ == "__main__":
    main()
