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
    logger.info("Checking if there is at least one file to upload...")
    uploaded_files = []  # Initialize the list here
    if not os.path.exists(args.config["input_path"]):
        raise Exception(f"Input path {args.config['input_path']} does not exist")
    has_files = False
    for root, dirs, files in os.walk(args.config["input_path"]):
        if len(files) > 0:
            has_files = True
            logger.info(f"Found {len(files)} files in {root}")
            break
    if not has_files:
        logger.info(f"No files to upload in {args.config['input_path']}")
        return
    
    logger.info(f"Exporting data...")
    config = args.config
    # Upload all data in input_path to sftp
    ## I don't think preserving directory structure matters, a nice to have, but error-prone
    sftp_conection = client.connection(config)
    sftp_client = sftp_conection.sftp
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
        
        logger.info(f"Root {root}. Dirs {dirs}. Files to upload: {files}")
        for file in files: #upload all files
            file_path = os.path.join(root, file)
            stripped_file_path = file_path.replace(config['input_path'] + "/", "",1)
            prev_cwd = None

            if "/" in stripped_file_path:
                prev_cwd = sftp_client.getcwd()
                # Go into the folder
                sftp_client.chdir(stripped_file_path.split("/")[0])

            # Add .tmp extension for upload
            temp_remote_file = f"{file}.tmp"
            
            logger.info(f"Uploading {file} with local path {file_path} to {config['path_prefix']} at {sftp_client.getcwd()} as {temp_remote_file}")

            if config.get("overwrite", False):
                # Check if the file exists on the remote server
                try:
                    sftp_client.stat(temp_remote_file)
                    sftp_client.remove(temp_remote_file)
                    logger.info(f"Removed existing temporary file: {temp_remote_file}")
                except FileNotFoundError:
                    pass

            confirm = config.get("confirm", True)
            if os.path.isfile(file_path):
                try:
                    sftp_client.put(file_path, temp_remote_file, confirm=confirm)
                    # Store the full path for renaming later
                    current_path = sftp_client.getcwd()
                    uploaded_files.append((current_path, temp_remote_file, file))
                except Exception as e:
                    logger.info(f"Failed while trying to upload file with remote path {file_path} to {config['path_prefix']} at {sftp_client.getcwd()}")
                    raise Exception(e)
            else:
                raise IOError(f'Could not find localFile {file_path} !!')

            if prev_cwd is not None:
                sftp_client.chdir(prev_cwd)

    # Rename all uploaded files by removing .tmp extension
    logger.info("All files uploaded. Renaming temporary files...")
    for path, temp_file, final_file in uploaded_files:
        try:
            sftp_client.chdir(path)
            
            # Check if final file already exists
            try:
                sftp_client.stat(final_file)
                if config.get("overwrite", False):
                    sftp_client.remove(final_file)
                    logger.info(f"Removed existing file: {final_file}")
                else:
                    raise Exception(f"File {final_file} already exists and overwrite is not enabled")
            except FileNotFoundError:
                pass  # File doesn't exist, which is fine
                
            sftp_client.rename(temp_file, final_file)
            logger.info(f"Renamed {temp_file} to {final_file}")
        except Exception as e:
            logger.error(f"Failed to rename {temp_file} to {final_file}: {str(e)}")
            raise e

    logger.info(f"Closing SFTP connection...")
    sftp_conection.close()


def main():
    # Parse command line arguments
    args = parse_args()

    # Upload the data
    upload(args)


if __name__ == "__main__":
    main()
