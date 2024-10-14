#!/usr/bin/env python3
import os
import json
import argparse
import logging
import threading
import paramiko
from time import sleep
from concurrent.futures import ThreadPoolExecutor
import threading
from target_sftp import client
import backoff

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

created = False
lock = threading.Lock()

def upload_part(sftp, num, offset, part_size, local_path, remote_path):
    global created
    logger.info(f"Running thread {num}")
    try:
        with open(local_path, "rb") as fl:
            fl.seek(offset)
            
            with lock:
                m = "r+" if created else "w"
                created = True
                try:
                    fr = sftp.open(remote_path, m)
                    with fr:
                        fr.seek(offset)
                        fr.set_pipelined(True)
                        size = 0
                        while size < part_size:
                            s = 32768
                            if size + s > part_size:
                                s = part_size - size
                            data = fl.read(s)
                            if len(data) == 0 or not data:
                                break
                            fr.write(data)
                            size += len(data)
                except (paramiko.sftp.SFTPError, OSError) as e:
                    logger.warning(f"Thread {num}: Error opening remote file, retrying. Error: {e}")
                    sleep(5)
                    _,_, sftp = start_sftp()
                    fr = sftp.open(remote_path, m)
    except (paramiko.ssh_exception.SSHException) as x:
        logger.info(f"Thread {num} failed: {x}")
    logger.info(f"Thread {num} done")


def upload_file_in_chunks(sftp_client, local_path, remote_path, chunk_size=1048576):
    logger.info("Starting to upload file...")
    offset = 0
    threads_count = 5
    size = os.path.getsize(local_path)
    part_size = int(size / threads_count)

    logger.info("Starting uploading file in chunks...")
    with ThreadPoolExecutor(max_workers=threads_count) as executor:
        futures = []
        for num in range(threads_count):
            part_size_adjusted = part_size if num < threads_count - 1 else size - offset
            args = (sftp_client, num, offset, part_size_adjusted, local_path, remote_path)
            logger.info(f"Starting thread {num} offset {offset} size {part_size_adjusted}")
            logger.debug(f"1. Active threads: {threading.enumerate()}")
            futures.append(executor.submit(upload_part, *args))
            offset += part_size

        for future in futures:
            logger.debug(f"2. Active threads: {threading.enumerate()}")
            future.result()
            logger.debug(f"3. Active threads: {threading.enumerate()}")

    logger.info("Upload file in chunks, all threads done")


def upload():
    logger.info(f"Initializing sftp server...")
    config, sftp_conection, sftp_client = start_sftp()
    logger.info(f"Exporting data...")
    output_path = config["path_prefix"]
    export_path = output_path.lstrip("/").rstrip("/")
    if not export_path:
        #Set default path to root
        export_path = "/"

    if export_path:
        for dir in export_path.split("/"):
            try:
                # First attempt to create the folder
                try:
                    sftp_client.mkdir(dir)
                except Exception as e:
                    # logger.exception(f"Failed to create folder {dir} in path {sftp_client.getcwd()}. See details below")
                    # If it already exists, we ignore
                    pass

                # Switch into the dir if it exists
                sftp_client.chdir(dir) #will change if folder already exists
            except Exception as e:
                # logger.exception(f"Failed to create folder {dir} in path {sftp_client.getcwd()}. See details below")
                raise e

    for root, dirs, files in os.walk(config["input_path"]):
        for dir in dirs:
            try:
                sftp_client.mkdir(dir)
                logger.info(f"Created remote folder {dir}")
            except:
                logger.info(f"Remote folder {dir} already exists")
        if isinstance(files,list) and len(files) == 0:
            logger.info(f"No files in {root}. Skipping...")
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

            # if we should overwrite files we should purge existing one before upload
            if config.get("overwrite", False):
                # Check if the file exists on the remote server
                try:
                    sftp_client.stat(file)
                    file_exists = True
                except FileNotFoundError:
                    file_exists = False

                # If the file exists, delete it
                if file_exists:
                    sftp_client.remove(file)
                    logger.info(f"Removed existing file: {file}")

            upload_file_in_chunks(sftp_client, file_path, file)

            if prev_cwd is not None:
                sftp_client.chdir(prev_cwd)
        
    logger.info(f"Closing SFTP connection...")
    sftp_conection.close()

@backoff.on_exception(backoff.expo, paramiko.ssh_exception.SSHException, max_tries=5)
def start_sftp():
    args = parse_args()
    config = args.config
    # Upload all data in input_path to sftp
    ## I don't think preserving directory structure matters, a nice to have, but error-prone
    sftp_conection = client.connection(config)
    sftp_client = sftp_conection.sftp
    return config,sftp_conection,sftp_client


def main():
    # Upload the data
    upload()


if __name__ == "__main__":
    main()
