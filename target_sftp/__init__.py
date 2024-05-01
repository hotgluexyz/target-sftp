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
    ## I don't think preserving directory structure matters, a nice to have, but error-prone
    sftp_conection = client.connection(config)
    sftp_client = sftp_conection.sftp
    output_path = config["path_prefix"]
    export_path = output_path.lstrip("/").rstrip("/")

    if export_path:
        for dir in export_path.split("/"):
            try:
                # First attempt to create the folder
                try:
                    sftp_client.mkdir(dir)
                except Exception as e:
                    logger.exception(f"Failed to create folder {dir} in path {sftp_client.getcwd()}. See details below")
                    # If it already exists, we ignore
                    pass

                # Switch into the dir if it exists
                sftp_client.chdir(dir) #will change if folder already exists
            except Exception as e:
                logger.exception(f"Failed to create folder {dir} in path {sftp_client.getcwd()}. See details below")
                raise e

    for root, dirs, files in os.walk(config["input_path"]):
        for dir in dirs:
            try:
                sftp_client.mkdir(dir)
                logger.info(f"Created remote folder {dir}")
            except:
                logger.info(f"Remote folder {dir} already exists")
        
        for file in files: #upload all files
            file_path = os.path.join(root, file)
            stripped_file_path = file_path.replace(config['input_path'] + "/", "",1)
            prev_cwd = None

            if "/" in stripped_file_path:
                prev_cwd = sftp_client.getcwd()
                # Go into the folder
                sftp_client.chdir(stripped_file_path.split("/")[0])

            # Save the file
            logger.info(f"Uploading {file} to {config['path_prefix']} at {sftp_client.getcwd()}")
            sftp_client.put(file_path, file)

            if prev_cwd is not None:
                sftp_client.chdir(prev_cwd)
        
    logger.info(f"Closing SFTP connection...")
    sftp_conection.close()


def main():
    # Parse command line arguments
    args = parse_args()

    # Upload the data
    upload(args)


if __name__ == "__main__":
    main()
