# target-sftp

A Singer target for exporting data to SFTP servers. Uploads local directories to remote SFTP locations with transactional safety and support for multiple SSH key types.

## Requirements

Tested on Python 3.7, 3.10, and 3.14. Python 3.7 is end-of-life; a future `cryptography` release may drop support for it.

## Installation

1. Set Python version with pyenv (example):
```bash
pyenv local 3.10.19
```

2. Create and activate a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate
```

3. Install the package:
```bash
pip install -e .
```

## Usage

```bash
target-sftp -c /path/to/config.json
```

## Configuration

Copy the template configuration file and customize it:

```bash
cp templates/config.json .secrets/config.json
```

See [templates/README.md](templates/README.md) for detailed configuration options and examples.

## Features

- Recursive directory upload
- Transactional uploads with rollback on failure
- Support for RSA, DSS, ECDSA, and Ed25519 SSH keys
- Password or key-based authentication
- Configurable overwrite behavior

