"""
Polars JDBC Tools - A comprehensive toolkit for working with JDBC connections in AWS environments using Polars.

This package provides utilities for:
- Managing AWS Glue JDBC connections
- Exploring database schemas and tables
- Reading data from databases into Polars DataFrames
"""

# Import core components
# Import AWS components
from .aws import extract_jdbc_from_glue, get_engine_from_glue, list_glue_connections
from .core import JDBCConnectionError, get_sqlalchemy_url, parse_jdbc_url

# Import database components
from .database import (
    execute_query,
    get_table_sample,
    get_table_schema,
    get_table_stats,
    list_schemas,
    list_tables,
    read_sql_with_polars,
    read_table,
)

__all__ = [
    # Core
    "parse_jdbc_url",
    "get_sqlalchemy_url",
    "JDBCConnectionError",
    # AWS
    "list_glue_connections",
    "extract_jdbc_from_glue",
    "get_engine_from_glue",
    # Database
    "list_schemas",
    "list_tables",
    "get_table_schema",
    "get_table_sample",
    "read_table",
    "execute_query",
    "get_table_stats",
    "read_sql_with_polars",
]

__version__ = "0.1.0"
