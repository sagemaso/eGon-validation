# Installation & Configuration

## Install

```bash
pip install -e .

# With dev/test dependencies
pip install -e ".[test,dev]"
```

## Database Connection

Set via environment variables or `.env` file:

```bash
# Option 1: Full URL
DB_URL=postgresql://user:password@host:5432/database

# Option 2: Individual parameters
DB_HOST=localhost
DB_PORT=5432
DB_NAME=egon-data
DB_USER=postgres
DB_PASS=secret
```

## SSH Tunnel (optional)

For remote databases behind a firewall:

```bash
SSH_HOST=gateway.example.com
SSH_USER=username
SSH_KEY_FILE=~/.ssh/id_rsa
```

Use with `--with-tunnel` flag.

## Execution Settings

```bash
MAX_WORKERS=6              # Parallel rule execution threads
OUTPUT_DIR=./validation_runs
DEFAULT_TOLERANCE=0.0      # Acceptable deviation for numeric checks
```