# Configuration Template

This folder contains a template configuration file for target-sftp.

## Usage

1. Copy `config.json` to your desired location (e.g., `.secrets/config.json`)
2. Fill in the required values
3. Remove any authentication methods you're not using

## Configuration Options

### Required Fields

- **`host`**: SFTP server hostname or IP address
  - Example: `"sftp.example.com"` or `"192.168.1.100"`

- **`username`**: SFTP username for authentication

- **`input_path`**: Local directory path to scan and upload
  - Example: `"/Users/username/data/export"` or `"./local_files"`
  - The tool recursively scans this directory

- **`path_prefix`**: Remote directory path where files will be uploaded
  - Example: `"/uploads/data"` or `"/export"`
  - Must be an absolute path (starts with `/`)

### Optional Fields

- **`port`**: SFTP server port number (defaults to `22` if not specified)

- **`overwrite`**: Whether to overwrite existing files on the remote server
  - Defaults to `false` if not specified
  - When `false`, existing files are skipped
  - When `true`, existing files are backed up (as `.old.tmp`) and replaced

### Authentication (Choose ONE)

You must provide one of the following authentication methods:

1. **`password`**: Password for authentication
   - Use if not using key-based authentication
   - Example: `"password": "mypassword"`

2. **`private_key_file`**: Path to your private SSH key file
   - Supports RSA, DSS, ECDSA, and Ed25519 keys
   - Example: `"private_key_file": "~/.ssh/id_rsa"`
   - Can use `~` for home directory expansion

3. **`private_key`**: Private key content as a string
   - Embed the key directly in the config
   - Must escape newlines as `\n`
   - Example: `"private_key": "-----BEGIN OPENSSH PRIVATE KEY-----\n...\n-----END OPENSSH PRIVATE KEY-----"`

## Example Configurations

### Password Authentication
```json
{
  "host": "sftp.example.com",
  "username": "myuser",
  "password": "mypassword",
  "port": 22,
  "input_path": "/local/data",
  "path_prefix": "/remote/uploads",
  "overwrite": false
}
```

### Key File Authentication
```json
{
  "host": "sftp.example.com",
  "username": "myuser",
  "private_key_file": "~/.ssh/id_rsa",
  "port": 22,
  "input_path": "/local/data",
  "path_prefix": "/remote/uploads",
  "overwrite": true
}
```

### Embedded Key Authentication
```json
{
  "host": "sftp.example.com",
  "username": "myuser",
  "private_key": "-----BEGIN OPENSSH PRIVATE KEY-----\nb3BlbnNzaC1rZXktdjEAAAAA...\n-----END OPENSSH PRIVATE KEY-----",
  "port": 22,
  "input_path": "/local/data",
  "path_prefix": "/remote/uploads",
  "overwrite": false
}
```

## Notes

- When using `private_key`, ensure all newlines are escaped as `\n`
- The tool automatically detects key types (RSA, DSS, ECDSA, Ed25519)
- If using `private_key_file`, ensure the file has proper permissions (`chmod 600`)

