# Polars JDBC Tools

A comprehensive toolkit for working with JDBC connections in AWS environments using Polars.

## Installation

You can install the package directly from GitHub:

```bash
pip install git+https://github.com/mfilipelino/pjt.git
```

For development installations:

```bash
# Clone the repository
git clone https://github.com/mfilipelino/pjt.git
cd pjt

# Install with development dependencies using UV
python -m pip install uv
uv pip install -e ".[dev]"
```

## Features

- **Connection Management**: Extract and use JDBC connection details from AWS Glue
- **Database Exploration**: List schemas, tables, and examine table structures
- **Data Access**: Efficiently load data into Polars DataFrames

## Function Reference

For detailed documentation on all available functions, please see [FUNCTION_REFERENCE.md](FUNCTION_REFERENCE.md).

## Quick Start

```python
import polars_jdbc_tools as pjt

# List available Glue connections
connections = pjt.list_glue_connections()
print(f"Available connections: {connections}")

# Create an engine from a Glue connection
engine = pjt.get_engine_from_glue("my-postgres-connection")

# List tables in the public schema
tables = pjt.list_tables(engine, schema="public")
print(f"Tables: {tables}")

# Read data from a table
df = pjt.read_table(
    engine,
    "customers",
    columns=["customer_id", "name", "email"],
    filters="created_at > '2023-01-01'"
)

# Process with Polars
print(df.head())
```

## Development

### Running Tests

```bash
pytest
```

With coverage:

```bash
pytest --cov=polars_jdbc_tools
```

### Linting

```bash
ruff check .
```

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.