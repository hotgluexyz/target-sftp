#!/usr/bin/env python3
import os
import json
import argparse
import logging
from typing import List, Tuple, Dict, Any, Optional, Iterator
import stat
from paramiko import SFTPClient

from target_sftp import client

logger = logging.getLogger("target-sftp")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def load_json(path: str) -> Dict[str, Any]:
    with open(path) as f:
        return json.load(f)

class FolderTree:
    class File:
        def __init__(self, name: str, local_path: str):
            self.name = name
            self.local_path = local_path
            self.remote_path = None  # Initialize remote_path
            self.should_be_copied = True 
            self.status = 'pending'  # Can be 'pending', 'uploading', 'uploaded', 'failed'

        def to_dict(self):
            return {
                "type": "File",
                "name": self.name,
                "local_path": self.local_path,
                "remote_path": self.remote_path,
                "should_be_copied": self.should_be_copied, 
                "status": self.status
            }

        def __str__(self):
            return json.dumps(self.to_dict())
        
        def localize(self, remote_path: str, overwrite: bool = False) -> None:
            if self.remote_path is not None:
                raise Exception(f"File {self.name} already has a remote path {self.remote_path}")
            self.remote_path = remote_path
            if overwrite:
                self.should_be_copied = False 

    class Folder:
        def __init__(self, name: str, parent: Optional['Folder'], relative_path_from_root: str):
            self.name = name
            self.path = relative_path_from_root
            self.parent = parent
            self.nested_folders: List[FolderTree.Folder] = []
            self.files: List[FolderTree.File] = []  # Add files list to store File objects

        def to_dict(self):
            return {
                "type": "Folder",
                "name": self.name,
                "path": self.path,
                "nested_folders": [folder.to_dict() for folder in self.nested_folders],
                "files": [file.to_dict() for file in self.files]
            }

        def __str__(self):
            return json.dumps(self.to_dict(), indent=2)

        @property
        def is_root(self):
            return self.parent is None

        @property
        def is_leaf(self):
            return len(self.nested_folders) == 0

        def add_nested_folder(self, folder: 'Folder') -> None:
            self.nested_folders.append(folder)

        def get_nested_folder(self, name: str) -> Optional['Folder']:
            return next((folder for folder in self.nested_folders if folder.name == name), None)

        def add_file(self, file: 'File') -> None:
            self.files.append(file)

        def get_file(self, name: str) -> Optional['File']:
            return next((file for file in self.files if file.name == name), None)

def parse_args() -> argparse.Namespace:
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

def build_local_tree(root_path: str) -> FolderTree.Folder:
    """
    Build a FolderTree structure by scanning a local directory
    
    Args:
        root_path (str): Path to the root directory to scan
        
    Returns:
        FolderTree.Folder: Root folder containing the entire directory structure
    """
    # Create root folder
    root_folder = FolderTree.Folder("root", None, "")
    base_path_length = len(root_path.rstrip("/")) + 1  # +1 for the trailing slash
    
    # Walk through directory
    for dirpath, dirnames, filenames in os.walk(root_path):
        # Calculate relative path from root
        rel_path = dirpath[base_path_length:] if dirpath[base_path_length:] else ""
        
        # Get or create current folder
        current_folder = root_folder
        if rel_path:
            path_parts = rel_path.split('/')
            for part in path_parts:
                next_folder = current_folder.get_nested_folder(part)
                if not next_folder:
                    next_folder = FolderTree.Folder(part, current_folder, 
                                                  os.path.join(current_folder.path, part))
                    current_folder.add_nested_folder(next_folder)
                current_folder = next_folder
        
        # Add files to current folder
        for filename in filenames:
            local_path = os.path.join(dirpath, filename)
            file_obj = FolderTree.File(filename, local_path)
            current_folder.add_file(file_obj)
    
    return root_folder

def build_remote_tree(sftp_client: SFTPClient, root_path: str) -> FolderTree.Folder:
    """
    Build a FolderTree structure by scanning a remote SFTP directory
    
    Args:
        sftp_client: Paramiko SFTP client instance
        root_path (str): Path to the root directory to scan on remote server
        
    Returns:
        FolderTree.Folder: Root folder containing the entire directory structure
    """
    # Create root folder
    root_folder = FolderTree.Folder("root", None, "")
    
    # Store original working directory
    original_path = sftp_client.getcwd()
    
    try:
        # Change to root path if it's not empty
        if root_path and root_path != "/":
            sftp_client.chdir(root_path)
        
        def scan_directory(current_path, parent_folder):
            try:
                # List directory contents
                for entry in sftp_client.listdir_attr(current_path):
                    name = entry.filename
                    full_path = os.path.join(current_path, name)
                    
                    # Skip hidden files/directories
                    if name.startswith('.'):
                        continue
                    
                    if stat.S_ISDIR(entry.st_mode):
                        # Create new folder
                        relative_path = os.path.relpath(full_path, root_path) if root_path != "/" else full_path
                        new_folder = FolderTree.Folder(name, parent_folder, relative_path)
                        parent_folder.add_nested_folder(new_folder)
                        
                        # Recursively scan subdirectory
                        scan_directory(full_path, new_folder)
                    else:
                        # Create file object
                        file_obj = FolderTree.File(name, None)  # local_path is None for remote files
                        file_obj.remote_path = full_path
                        file_obj.should_be_copied = False
                        parent_folder.add_file(file_obj)
                        
            except IOError as e:
                logger.error(f"Error scanning directory {current_path}: {str(e)}")
                raise
        
        # Start scanning from root
        current_path = "." if root_path == "/" else root_path
        scan_directory(current_path, root_folder)
        
    finally:
        # Restore original working directory
        sftp_client.chdir(original_path)
    
    return root_folder

def upload(args: argparse.Namespace) -> None:
    logger.info("Checking if there is at least one file to upload...")
    if not has_minimum_amount_of_files(args):
        return

    logger.info(f"Exporting data...")
    config = args.config
    sftp_connection = client.connection(config)
    sftp_client = sftp_connection.sftp
    
    try:
        output_path = config["path_prefix"]
        export_path = output_path.rstrip("/") or "/"

        local_tree = build_local_tree(config["input_path"])
        logger.debug(f"Local directory tree structure: {local_tree}")
        
        remote_tree = build_remote_tree(sftp_client, export_path)
        logger.debug(f"Remote directory tree structure: {remote_tree}")
        
        prepared_tree = prepare_upload_tree(local_tree, remote_tree, export_path, 
                                          overwrite=config.get("overwrite", False))
        logger.debug(f"Prepared directory tree structure: {prepared_tree}")
        
        execute_upload(sftp_client, prepared_tree, config.get("overwrite", False))
        logger.info("Upload completed successfully")
        
    finally:
        logger.info("Closing SFTP connection...")
        sftp_connection.close()

def has_minimum_amount_of_files(args: argparse.Namespace) -> bool:
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
        return False
    return True

def prepare_upload_tree(
    local_tree: 'FolderTree.Folder',
    remote_tree: 'FolderTree.Folder',
    root_path: str,
    overwrite: bool = False
) -> 'FolderTree.Folder':
    """
    Prepare the local directory tree for upload by:
    1. Setting remote paths for all local files
    2. Determining which files need to be copied based on remote tree comparison
    
    Args:
        local_tree: Local directory tree to be uploaded
        remote_tree: Existing remote directory tree
        root_path: Remote root path where files will be uploaded
        overwrite: Whether to mark existing files for overwrite
    
    Returns:
        FolderTree.Folder: Local tree prepared for upload with remote paths set
                          and should_be_copied flags configured
    """
    def find_remote_folder(current_remote_folder: 'FolderTree.Folder', 
                          path_parts: List[str]) -> Optional['FolderTree.Folder']:
        if not path_parts:
            return current_remote_folder
        next_folder = current_remote_folder.get_nested_folder(path_parts[0])
        if next_folder is None:
            return None
        return find_remote_folder(next_folder, path_parts[1:])

    def localize_files(local_folder: 'FolderTree.Folder', current_path: str) -> None:
        # Find corresponding remote folder
        rel_path = local_folder.path.split('/') if local_folder.path else []
        remote_folder = find_remote_folder(remote_tree, rel_path)

        # Localize files in current folder
        for file in local_folder.files:
            if file.remote_path is None:  # Only localize files that don't have a remote path
                remote_file_path = os.path.join(current_path, file.name)
                file.remote_path = remote_file_path
                
                # Check if file exists in remote folder and set should_be_copied accordingly
                existing_remote = remote_folder.get_file(file.name) if remote_folder else None
                if existing_remote is not None:
                    if not overwrite:
                        file.should_be_copied = False
                        logger.info(f"Skipping file {file.local_path} -> {file.remote_path} (already exists and overwrite=False)")
                    else:
                        logger.info(f"Will overwrite existing file {file.remote_path}")
                else:
                    logger.info(f"Will upload new file {file.local_path} -> {file.remote_path}")
        
        # Recursively process nested folders
        for nested in local_folder.nested_folders:
            nested_path = os.path.join(current_path, nested.name)
            localize_files(nested, nested_path)
    
    # First, localize all files in the local tree with their future remote paths
    root_path = root_path.rstrip('/')
    localize_files(local_tree, root_path)
    
    return local_tree

def cleanup_previous_artifacts(sftp_client: SFTPClient, root_path: str) -> None:
    """
    Clean up any artifacts from previous failed uploads in this order:
    1. Restore any .target_old files to their original names
    2. Remove any remaining .target_tmp files
    3. Remove empty _target_tmp directories
    
    Args:
        sftp_client: Paramiko SFTP client instance
        root_path: Root path to start cleaning from
    """
    def cleanup_directory(path: str) -> None:
        try:
            # First, get all entries to separate files and directories
            entries = sftp_client.listdir_attr(path)
            files = []
            dirs = []
            
            for entry in entries:
                name = entry.filename
                full_path = os.path.join(path, name)
                
                if stat.S_ISDIR(entry.st_mode):
                    dirs.append((full_path, name))
                else:
                    files.append((full_path, name))
            
            # Step 1: Restore .target_old files first
            for full_path, name in files:
                if name.endswith('.target_old'):
                    try:
                        original_path = full_path[:-11]  # Remove '.target_old'
                        sftp_client.rename(full_path, original_path)
                        logger.info(f"Cleanup: Restored {full_path} to {original_path}")
                    except Exception as e:
                        logger.warning(f"Cleanup: Failed to restore {full_path}: {str(e)}")
            
            # Step 2: Remove .target_tmp files
            for full_path, name in files:
                if name.endswith('.target_tmp'):
                    try:
                        sftp_client.remove(full_path)
                        logger.info(f"Cleanup: Removed leftover file {full_path}")
                    except Exception as e:
                        logger.warning(f"Cleanup: Failed to remove file {full_path}: {str(e)}")
            
            # Step 3: Recursively process directories
            for full_path, name in dirs:
                cleanup_directory(full_path)
                
            # Step 4: After processing contents, check if this is a temporary directory
            if path.endswith('_target_tmp'):
                try:
                    if not sftp_client.listdir(path):  # Only if empty
                        sftp_client.rmdir(path)
                        logger.info(f"Cleanup: Removed empty temporary directory {path}")
                except Exception as e:
                    logger.warning(f"Cleanup: Failed to remove directory {path}: {str(e)}")
        
        except IOError as e:
            logger.error(f"Cleanup: Error accessing directory {path}: {str(e)}")

    logger.info("Starting cleanup of previous upload artifacts...")
    logger.info("Step 1: Restoring any .target_old files to their original names...")
    logger.info("Step 2: Removing any remaining .target_tmp files...")
    logger.info("Step 3: Removing empty _target_tmp directories...")
    cleanup_directory(root_path)
    logger.info("Cleanup completed")

def execute_upload(sftp_client: SFTPClient, prepared_tree: 'FolderTree.Folder', overwrite: bool = False) -> None:
    """
    Execute the upload process with transactional behavior.
    
    Process:
    1. Clean up any artifacts from previous failed uploads
    2. Upload all files as .target_tmp
    3. If overwrite enabled, rename existing files to .target_old
    4. Rename .target_tmp files to their final names
    5. Clean up .target_old files
    
    Args:
        sftp_client: Paramiko SFTP client instance
        prepared_tree: Tree prepared for upload
        overwrite: Whether to overwrite existing files
    
    Raises:
        Exception: If any step fails, triggers rollback of all changes
    """
    uploaded_tmp_files: List[Tuple[str, str, str]] = []  # (path, tmp_filename, final_filename)
    renamed_old_files: List[Tuple[str, str]] = []   # (path, filename)
    created_directories: List[str] = []  # Full paths of created directories
    
    try:
        # Step 1: Clean up any artifacts from previous failed uploads
        cleanup_previous_artifacts(sftp_client, os.path.dirname(prepared_tree.files[0].remote_path) if prepared_tree.files else "/")
        
        # Step 2: Upload all files as .target_tmp
        def upload_folder(folder: 'FolderTree.Folder') -> None:
            for file in folder.files:
                if not file.should_be_copied:
                    continue
                    
                tmp_path = f"{file.remote_path}.target_tmp"
                logger.info(f"Uploading {file.local_path} to {tmp_path}")
                
                # Create parent directories if they don't exist
                parent_dir = os.path.dirname(file.remote_path)
                try:
                    sftp_client.stat(parent_dir)
                except FileNotFoundError:
                    # Create parent directories recursively
                    dirs_to_create = []
                    current_dir = parent_dir
                    while current_dir:
                        try:
                            sftp_client.stat(current_dir)
                            break
                        except FileNotFoundError:
                            dirs_to_create.append(current_dir)
                            current_dir = os.path.dirname(current_dir)
                    
                    # Create directories from root to leaf
                    for dir_path in reversed(dirs_to_create):
                        sftp_client.mkdir(dir_path)
                        created_directories.append(dir_path)
                        logger.info(f"Created directory {dir_path}")
                
                # Upload with .target_tmp extension
                sftp_client.put(file.local_path, tmp_path)
                uploaded_tmp_files.append((
                    os.path.dirname(file.remote_path),
                    os.path.basename(tmp_path),
                    os.path.basename(file.remote_path)
                ))
                
            for subfolder in folder.nested_folders:
                upload_folder(subfolder)
        
        upload_folder(prepared_tree)
        logger.info(f"Successfully uploaded {len(uploaded_tmp_files)} temporary files")
        
        # Step 3: If overwrite enabled, rename existing files to .target_old
        if overwrite:
            for path, _, final_name in uploaded_tmp_files:
                try:
                    original_path = os.path.join(path, final_name)
                    old_path = f"{original_path}.target_old"
                    
                    # Check if file exists before renaming
                    try:
                        sftp_client.stat(original_path)
                        sftp_client.rename(original_path, old_path)
                        renamed_old_files.append((path, final_name))
                        logger.info(f"Renamed existing file {original_path} to {old_path}")
                    except FileNotFoundError:
                        logger.info(f"File {original_path} not found, skipping rename... It was probably deleted by another process before we could rename it")
                        pass
                except Exception as e:
                    raise Exception(f"Failed to rename existing file {original_path}: {str(e)}")
        
        # Step 4: Rename .target_tmp files to final names
        for path, tmp_name, final_name in uploaded_tmp_files:
            try:
                tmp_path = os.path.join(path, tmp_name)
                final_path = os.path.join(path, final_name)
                sftp_client.rename(tmp_path, final_path)
                logger.info(f"Renamed {tmp_path} to {final_path}")
            except Exception as e:
                raise Exception(f"Failed to rename temporary file {tmp_path}: {str(e)}")
        
        # Step 5: Clean up .target_old files
        for path, filename in renamed_old_files:
            try:
                old_path = os.path.join(path, f"{filename}.target_old")
                sftp_client.remove(old_path)
                logger.info(f"Removed old file {old_path}")
            except Exception as e:
                logger.warning(f"Failed to remove old file {old_path}: {str(e)}")
                
    except Exception as e:
        logger.error("Upload failed, initiating rollback...")
        
        # Rollback: Remove all .target_tmp files
        for path, tmp_name, _ in uploaded_tmp_files:
            try:
                tmp_path = os.path.join(path, tmp_name)
                sftp_client.remove(tmp_path)
                logger.info(f"Rollback: Removed temporary file {tmp_path}")
            except:
                logger.warning(f"Rollback: Failed to remove temporary file {tmp_path}")
        
        # Rollback: Restore .target_old files to their original names
        for path, filename in renamed_old_files:
            try:
                old_path = os.path.join(path, f"{filename}.target_old")
                original_path = os.path.join(path, filename)
                sftp_client.rename(old_path, original_path)
                logger.info(f"Rollback: Restored {old_path} to {original_path}")
            except:
                logger.warning(f"Rollback: Failed to restore {old_path}")
        
        # Rollback: Remove created directories in reverse order (leaf to root)
        for dir_path in reversed(created_directories):
            try:
                # Check if directory is empty before removing
                try:
                    if not sftp_client.listdir(dir_path):
                        sftp_client.rmdir(dir_path)
                        logger.info(f"Rollback: Removed empty directory {dir_path}")
                    else:
                        logger.warning(f"Rollback: Directory not empty, keeping {dir_path}")
                except FileNotFoundError:
                    # Directory already removed or doesn't exist
                    pass
            except Exception as e:
                logger.warning(f"Rollback: Failed to remove directory {dir_path}: {str(e)}")
        
        raise Exception(f"Upload failed and was rolled back: {str(e)}")

def main() -> None:
    args = parse_args()
    upload(args)

if __name__ == "__main__":
    main()
