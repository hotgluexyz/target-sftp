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



def upload(args):
    logger.info(f"Exporting data...")
    config = args.config

    # Upload all data in input_path to sftp
    sftp_conection = client.connection(config)
    sftp_client = sftp_conection.sftp
    output_path = config["path_prefix"]
    try: 
        # check if structure exists before changing
        sftp_client.chdir(output_path) #will change if folder already exists
    except IOError:
        sftp_client.mkdir(output_path)
        logger.info(f"Creating output path at {sftp_client.getcwd()}")
        sftp_client.chdir(output_path)


    for root, dirs, files in os.walk(config["input_path"]):
        head, cwd = os.path.split(root) #get cwd in tree
        if cwd:
            sftp_client.chdir(cwd) #put sftp in the same place

        for dir in dirs:
            if dir not in sftp_client.listdir(): #check to see if directory already in sftp, make if it isn't there
                logger.info(f"Creating folder {dir} at {sftp_client.getcwd()}")
                sftp_client.mkdir(dir)

        
        for file in files: #upload all files
            file_path = os.path.join(root, file)
            logger.info(f"Uploading {file} to {config['path_prefix']} at {sftp_client.getcwd()}")
            sftp_client.put(file_path, file)
        
        if cwd and not dirs: ## If dirs is empty then there are NO subfolders that need to be populated, safe to go back
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
