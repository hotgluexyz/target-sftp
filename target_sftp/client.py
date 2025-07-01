import logging
import os
import re
import stat
import time
from io import StringIO
import backoff
import paramiko
import singer
from paramiko.ssh_exception import AuthenticationException, SSHException

LOGGER = singer.get_logger()

logging.getLogger("paramiko").setLevel(logging.CRITICAL)

def handle_backoff(details):
    LOGGER.warn(
        "SSH Connection closed unexpectedly. Waiting {wait} seconds and retrying...".format(**details)
    )


class SFTPConnection():
    def __init__(self, host, username, password=None, private_key_file=None, private_key=None, port=None):
        self.host = host
        self.username = username
        self.password = password
        self.port = int(port or 22)
        self.key = None
        self.transport = None
        self.retries = 10
        self.__sftp = None
        if private_key_file:
            key_path = os.path.expanduser(private_key_file)
            self.key = paramiko.RSAKey.from_private_key_file(key_path)
        if private_key:
            key_string = StringIO(private_key)
            self.key = paramiko.RSAKey.from_private_key(key_string)
    # If connection is snapped during connect flow, retry up to a
    # minute for SSH connection to succeed. 2^6 + 2^5 + ...
    @backoff.on_exception(
        backoff.expo,
        (EOFError, ConnectionResetError),
        max_tries=6,
        on_backoff=handle_backoff,
        jitter=None,
        factor=2)
    def __connect(self):
        for i in range(self.retries+1):
            try:
                LOGGER.info('Creating new connection to SFTP...')
                self.transport = paramiko.Transport((self.host, self.port))
                self.transport.use_compression(True)
                self.transport.connect(username=self.username, password=self.password, hostkey=None, pkey=self.key)
                self.__sftp = paramiko.SFTPClient.from_transport(self.transport)
                LOGGER.info('Connection successful')
                break
            except (AuthenticationException, SSHException, ConnectionResetError) as ex:
                self.close()
                LOGGER.info(f'Connection failed, retrying after {5*i} seconds...')
                time.sleep(5*i)
                LOGGER.info('Retrying now')
                if i >= (self.retries):
                    raise ex

    @property
    def sftp(self):
        self.__connect()
        return self.__sftp

    @sftp.setter
    def sftp(self, sftp):
        self.__sftp = sftp

    def close(self):
        """Close SFTP connection with proper error handling"""
        if self.__sftp:
            try:
                self.__sftp.close()
                LOGGER.info("SFTP client closed successfully")
            except Exception as e:
                LOGGER.warning(f"Error closing SFTP client: {str(e)}")
            finally:
                self.__sftp = None
        
        if self.transport:
            try:
                self.transport.close()
                LOGGER.info("Transport closed successfully")
            except Exception as e:
                LOGGER.warning(f"Error closing transport: {str(e)}")
            finally:
                self.transport = None

    def match_files_for_table(self, files, table_name, search_pattern):
        LOGGER.info("Searching for files for table '%s', matching pattern: %s", table_name, search_pattern)
        matcher = re.compile(search_pattern)
        return [f for f in files if matcher.search(f["filepath"])]

    def is_empty(self, file_attr):
        return file_attr.st_size == 0

    def is_directory(self, file_attr):
        return stat.S_ISDIR(file_attr.st_mode)

    def __enter__(self):
        """Context manager entry point"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit point - ensures cleanup happens"""
        self.close()
        return False  # Don't suppress exceptions

def connection(config):
    return SFTPConnection(config['host'],
                          config['username'],
                          password=config.get('password'),
                          private_key_file=config.get('private_key_file'),
                          private_key = config.get('private_key'),
                          port=config.get('port'))
