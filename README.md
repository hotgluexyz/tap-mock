# tap-mock

A Singer.io tap for generating mock data for testing purposes. This tap generates deterministic mock data for customers and opportunities, supporting both OAuth and API key authentication methods.

## Features

- **Mock Data Generation**: Generates realistic mock data for customers and opportunities
- **Multiple Authentication Methods**: Supports both OAuth and API key authentication
- **Incremental Sync**: Supports incremental replication using bookmarks
- **Deterministic Data**: Generates consistent data for testing scenarios
- **Token Rotation**: Simulates OAuth token refresh functionality with `next_refresh_token`
- **Proper Singer Protocol**: Generates correct SCHEMA, RECORD, and STATE messages

## Installation

### From Source

```bash
# Clone the repository
git clone <repository-url>
cd tap-mock

# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install in development mode
pip install -e .
```

### From PyPI (when published)

```bash
pip install tap-mock
```

## Configuration

Create a single `config.json` file with all attributes at the root level. The tap will use the appropriate attributes based on the `auth_type`.

### OAuth Configuration

```json
{
  "auth_type": "oauth",
  "client_id": "your_client_id",
  "client_secret": "your_client_secret",
  "refresh_token": "your_refresh_token",
  "rotate_refresh_token": false,
  "next_refresh_token": "your_next_refresh_token",
  "access_token": "your_access_token"
}
```

### API Key Configuration

```json
{
  "auth_type": "api_key",
  "api_key": "your_api_key"
}
```

### Configuration Attributes

- **`auth_type`**: Either `"oauth"` or `"api_key"` (required)
- **`client_id`**: OAuth client ID (required for OAuth)
- **`client_secret`**: OAuth client secret (required for OAuth)
- **`refresh_token`**: OAuth refresh token (required for OAuth)
- **`rotate_refresh_token`**: Enable refresh token rotation (optional, default: false)
- **`next_refresh_token`**: Next refresh token for rotation (required when `rotate_refresh_token` is true)
- **`access_token`**: OAuth access token (optional for OAuth)
- **`api_key`**: API key value (required for API key auth)

## Usage

### Discover Streams

```bash
tap-mock --config config.json --discover
```

This generates a Singer catalog with proper metadata structure including field-level metadata and `selected: false` by default.

### Sync Data

```bash
# Sync all streams (full sync)
tap-mock --config config.json

# Sync with catalog (selective sync)
tap-mock --config config.json --catalog catalog.json

# Sync with state (incremental sync)
tap-mock --config config.json --state state.json

# Sync with both catalog and state
tap-mock --config config.json --catalog catalog.json --state state.json
```

### Using singer-catalog-select

```bash
# Generate catalog
tap-mock --config config.json --discover > catalog.json

# Select specific streams
singer-catalog-select --catalog catalog.json --streams customers > catalog-selected.json

# Select specific fields
singer-catalog-select --catalog catalog.json --streams customers --fields id,name,email > catalog-fields.json
```

## Streams

### Customers

Generates mock customer data with the following fields:
- `id`: Unique customer identifier (format: CUST_XXXXXX)
- `name`: Customer name
- `email`: Customer email address
- `status`: Customer status (active, inactive, pending)
- `created_at`: Creation timestamp (timezone-aware)
- `updated_at`: Last update timestamp (timezone-aware)
- `metadata`: Additional metadata with source and generation timestamp

**Incremental Sync Behavior:**
- **Full Sync**: Generates 100 customer records
- **Incremental Sync**: Generates 5 updated customers (with changed emails) + 5 new customers

### Opportunities

Generates mock opportunity data with the following fields:
- `id`: Unique opportunity identifier (format: OPP_XXXXXX)
- `name`: Opportunity name
- `customer_id`: Associated customer ID
- `amount`: Opportunity amount (increasing values)
- `stage`: Sales stage (prospecting, qualification, proposal, negotiation, closed)
- `probability`: Win probability percentage (1-100)
- `created_at`: Creation timestamp (timezone-aware)
- `updated_at`: Last update timestamp (timezone-aware)
- `metadata`: Additional metadata with source and generation timestamp

**Incremental Sync Behavior:**
- **Full Sync**: Generates 50 opportunity records
- **Incremental Sync**: Generates 1 new opportunity

## Authentication

### OAuth Authentication

The tap supports OAuth authentication with optional token rotation:

1. **Basic OAuth**: Uses `client_id`, `client_secret`, and `refresh_token`
2. **Token Rotation**: When `rotate_refresh_token` is true, requires `next_refresh_token`
   - If `next_refresh_token` is missing, throws an error
   - Updates the config file with the new refresh token

### API Key Authentication

Simple API key authentication using the `api_key` field.

## State Management

The tap generates proper Singer STATE messages for incremental replication:

```json
{
  "type": "STATE",
  "value": {
    "bookmarks": {
      "customers": {
        "last_updated": "2024-11-27T21:30:36.341714+00:00"
      },
      "opportunities": {
        "last_updated": "2025-04-01T21:30:36.342781+00:00"
      }
    }
  }
}
```

## Development

### Project Structure

```
tap-mock/
├── tap_mock/
│   └── __init__.py          # Main tap implementation
├── setup.py                 # Package setup with entry points
├── requirements.txt         # Dependencies
├── .gitignore              # Git ignore file
├── config.json             # Sample configuration
├── catalog.json            # Sample catalog
├── state.json              # Sample state
└── README.md               # This file
```

### Running Tests

```bash
# Run tests
python -m pytest test_tap_mock.py

# Run with coverage
python -m pytest test_tap_mock.py --cov=tap_mock
```

### Building and Installing

```bash
# Install in development mode
pip install -e .

# Build the package
python setup.py build

# Install the package
python setup.py install
```

### Virtual Environment

```bash
# Create virtual environment
python -m venv .venv

# Activate virtual environment
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Install tap in development mode
pip install -e .
```

## Examples

### Full Sync Example

```bash
# Generate all data
tap-mock --config config.json
```

Output: 100 customers + 50 opportunities

### Incremental Sync Example

```bash
# First run (creates state)
tap-mock --config config.json > output.json

# Extract state
tap-mock --config config.json | grep '"type": "STATE"' | tail -1 | jq -r '.value' > state.json

# Incremental run
tap-mock --config config.json --state state.json
```

Output: 10 customers (5 updates + 5 new) + 1 new opportunity

### Selective Sync Example

```bash
# Generate catalog
tap-mock --config config.json --discover > catalog.json

# Select only customers
singer-catalog-select --catalog catalog.json --streams customers > catalog-customers.json

# Sync only customers
tap-mock --config config.json --catalog catalog-customers.json
```

## License

[Add your license information here]

## Contributing

[Add contribution guidelines here] 