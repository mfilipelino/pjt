"""
Database interaction utilities for JDBC connections.

This module provides functions to interact with databases, such as listing schemas,
tables, and querying data using Polars.
"""

from typing import Any, Dict, List, Optional

import polars as pl
from sqlalchemy import inspect, text


def list_schemas(engine, exclude_system=True) -> List[str]:
    """
    List all schemas in a database.

    Args:
        engine: SQLAlchemy engine
        exclude_system (bool): Whether to exclude system schemas

    Returns:
        List[str]: List of schema names
    """
    inspector = inspect(engine)
    schemas = inspector.get_schema_names()

    if exclude_system:
        # Filter out system schemas based on database type
        conn_url = str(engine.url)
        if "postgresql" in conn_url or "redshift" in conn_url:
            # PostgreSQL and Redshift system schemas typically start with pg_, information_schema, etc.
            schemas = [
                s for s in schemas if not s.startswith(("pg_", "information_schema"))
            ]
        elif "mssql" in conn_url:
            # SQL Server system schemas
            system_schemas = [
                "sys",
                "INFORMATION_SCHEMA",
                "db_accessadmin",
                "db_backupoperator",
                "db_datareader",
                "db_datawriter",
                "db_ddladmin",
                "db_denydatareader",
                "db_denydatawriter",
                "db_owner",
                "db_securityadmin",
                "guest",
            ]
            schemas = [s for s in schemas if s not in system_schemas]

    return schemas


def list_tables(engine, schema=None, exclude_views=False) -> List[str]:
    """
    List all tables in a schema.

    Args:
        engine: SQLAlchemy engine
        schema (str, optional): Schema name. If None, uses default schema.
        exclude_views (bool): Whether to exclude views

    Returns:
        List[str]: List of table names
    """
    inspector = inspect(engine)

    if schema is None:
        # Determine default schema based on database type
        conn_url = str(engine.url)
        if "postgresql" in conn_url or "redshift" in conn_url:
            schema = "public"
        elif "mssql" in conn_url:
            schema = "dbo"

    if exclude_views:
        # Get only tables
        return inspector.get_table_names(schema)
    else:
        # Get tables and views
        tables = inspector.get_table_names(schema)
        views = inspector.get_view_names(schema)
        return tables + views


def get_table_schema(
    engine, table_name: str, schema: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Get schema information for a table.

    Args:
        engine: SQLAlchemy engine
        table_name (str): Table name
        schema (str, optional): Schema name. If None, uses default schema.

    Returns:
        List[Dict]: List of column information dictionaries
    """
    inspector = inspect(engine)
    return inspector.get_columns(table_name, schema)


def get_table_sample(
    engine, table_name: str, schema: Optional[str] = None, limit: int = 10
) -> pl.DataFrame:
    """
    Get a sample of data from a table using Polars.

    Args:
        engine: SQLAlchemy engine
        table_name (str): Table name
        schema (str, optional): Schema name. If None, uses default schema.
        limit (int): Number of rows to retrieve

    Returns:
        pl.DataFrame: Polars DataFrame containing sample data
    """
    qualified_table = f"{schema}.{table_name}" if schema else table_name
    query = f"SELECT * FROM {qualified_table} LIMIT {limit}"

    return pl.read_database(query=query, connection=engine)


def read_table(
    engine,
    table_name: str,
    schema: Optional[str] = None,
    columns: Optional[List[str]] = None,
    filters: Optional[str] = None,
    batch_size: int = 10000,
) -> pl.DataFrame:
    """
    Read a table into a Polars DataFrame.

    Args:
        engine: SQLAlchemy engine
        table_name (str): Table name
        schema (str, optional): Schema name. If None, uses default schema.
        columns (List[str], optional): List of column names to select. If None, selects all columns.
        filters (str, optional): SQL WHERE clause filters (without the 'WHERE' keyword)
        batch_size (int): Batch size for reading data

    Returns:
        pl.DataFrame: Polars DataFrame containing the data
    """
    # Build query
    qualified_table = f"{schema}.{table_name}" if schema else table_name

    # Handle column selection
    column_clause = "*"
    if columns:
        column_clause = ", ".join(columns)

    # Build query with or without filters
    if filters:
        query = f"SELECT {column_clause} FROM {qualified_table} WHERE {filters}"
    else:
        query = f"SELECT {column_clause} FROM {qualified_table}"

    # Use Polars to read the database
    return pl.read_database(query=query, connection=engine, batch_size=batch_size)


def execute_query(engine, query: str, batch_size: int = 10000) -> pl.DataFrame:
    """
    Execute a SQL query and return results as a Polars DataFrame.

    Args:
        engine: SQLAlchemy engine
        query (str): SQL query to execute
        batch_size (int): Batch size for reading data

    Returns:
        pl.DataFrame: Polars DataFrame containing the results
    """
    return pl.read_database(query=query, connection=engine, batch_size=batch_size)


def get_table_stats(
    engine, table_name: str, schema: Optional[str] = None
) -> Dict[str, Any]:
    """
    Get basic statistics about a table.

    Args:
        engine: SQLAlchemy engine
        table_name (str): Table name
        schema (str, optional): Schema name. If None, uses default schema.

    Returns:
        Dict[str, Any]: Dictionary containing table statistics
    """
    qualified_table = f"{schema}.{table_name}" if schema else table_name

    # Get column information
    columns = get_table_schema(engine, table_name, schema)

    # Get row count
    with engine.connect() as conn:
        count_query = f"SELECT COUNT(*) FROM {qualified_table}"
        row_count = conn.execute(text(count_query)).scalar()

    # Get table size info if possible (database specific)
    size_info = {}
    conn_url = str(engine.url)

    try:
        if "postgresql" in conn_url:
            # PostgreSQL size query
            size_query = f"""
                SELECT pg_size_pretty(pg_total_relation_size('{qualified_table}')) as total_size,
                       pg_size_pretty(pg_relation_size('{qualified_table}')) as table_size,
                       pg_size_pretty(pg_total_relation_size('{qualified_table}') - pg_relation_size('{qualified_table}')) as index_size
            """
            with engine.connect() as conn:
                result = conn.execute(text(size_query)).fetchone()
                if result:
                    size_info = {
                        "total_size": result[0],
                        "table_size": result[1],
                        "index_size": result[2],
                    }

        elif "redshift" in conn_url:
            # Redshift size query
            size_query = f"""
                SELECT COUNT(*) as row_count,
                       SUM(encoded_block_size)/(1024*1024) as size_mb
                FROM stv_blocklist b, stv_tbl_perm p
                WHERE b.tbl = p.id AND p.name = '{table_name}'
                  AND p.schema = '{schema or 'public'}'
            """
            with engine.connect() as conn:
                result = conn.execute(text(size_query)).fetchone()
                if result:
                    size_info = {"size_mb": result[1]}
    except Exception:
        # If size query fails, continue without size info
        pass

    # Return combined stats
    return {
        "table_name": table_name,
        "schema": schema,
        "row_count": row_count,
        "column_count": len(columns),
        "columns": columns,
        "size_info": size_info,
    }


def read_sql_with_polars(
    connection_name: str,
    query: str,
    region_name: Optional[str] = None,
    batch_size: int = 10000,
) -> pl.DataFrame:
    """
    Convenience function to read SQL directly using a Glue connection name.

    Args:
        connection_name (str): Glue connection name
        query (str): SQL query to execute
        region_name (str, optional): AWS region name
        batch_size (int): Batch size for reading data

    Returns:
        pl.DataFrame: Polars DataFrame containing query results
    """
    from .aws import get_engine_from_glue

    engine = get_engine_from_glue(connection_name, region_name)
    return pl.read_database(query=query, connection=engine, batch_size=batch_size)
