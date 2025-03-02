"""
Polars JDBC Connection Utilities for AWS Notebooks

This module provides a set of utility functions to work with JDBC connections in AWS environments,
particularly for use with Polars in notebooks. It can extract connection details from AWS Glue
connections and facilitate data operations across PostgreSQL, SQL Server, and Redshift databases.
"""

import boto3
import json
import os
from urllib.parse import urlparse, parse_qs, quote_plus
from typing import Dict, List, Any, Optional, Tuple

import polars as pl
from sqlalchemy import create_engine, inspect, Table, MetaData, select, text


class JDBCConnectionError(Exception):
    """Exception raised for errors in JDBC connections."""
    pass


def list_glue_connections(region_name=None) -> List[str]:
    """
    List all AWS Glue connection names in a region.
    
    Args:
        region_name (str, optional): AWS region name. If None, uses default from AWS config.
        
    Returns:
        List[str]: List of connection names
    """
    try:
        # Initialize Glue client
        glue_client = boto3.client('glue', region_name=region_name)
        
        # Get list of connections (paginated results)
        connection_names = []
        paginator = glue_client.get_paginator('get_connections')
        
        for page in paginator.paginate():
            # Extract just the names from each connection
            names = [conn.get('Name') for conn in page.get('ConnectionList', [])]
            connection_names.extend(names)
        
        return connection_names
            
    except Exception as e:
        raise JDBCConnectionError(f"Error listing connections: {str(e)}")


def extract_jdbc_from_glue(connection_name: str, region_name: Optional[str] = None) -> Dict[str, Any]:
    """
    Extract JDBC connection details from an AWS Glue Connection.
    
    Args:
        connection_name (str): Name of the Glue connection
        region_name (str, optional): AWS region name. If None, uses default from AWS config.
        
    Returns:
        dict: Dictionary containing connection details (connection_type, host, port, database, user, password)
    """
    try:
        # Initialize Glue client
        glue_client = boto3.client('glue', region_name=region_name)
        
        # Get connection details
        response = glue_client.get_connection(Name=connection_name)
        
        # Check if connection exists
        if 'Connection' not in response:
            raise JDBCConnectionError(f"Connection '{connection_name}' not found")
        
        connection = response['Connection']
        connection_properties = connection.get('ConnectionProperties', {})
        
        # Check if it's a JDBC connection
        if 'JDBC_CONNECTION_URL' not in connection_properties:
            raise JDBCConnectionError(f"Connection '{connection_name}' is not a JDBC connection")
        
        # Extract JDBC URL
        jdbc_url = connection_properties['JDBC_CONNECTION_URL']
        
        # Check if it's a valid JDBC URL
        if not jdbc_url.startswith('jdbc:'):
            raise JDBCConnectionError(f"Invalid JDBC URL format: {jdbc_url}")
        
        # Get username and password
        username = connection_properties.get('USERNAME', '')
        password = connection_properties.get('PASSWORD', '')
        
        # Parse connection details based on database type
        connection_details = parse_jdbc_url(jdbc_url)
        connection_details['user'] = username
        connection_details['password'] = password
        connection_details['jdbc_url'] = jdbc_url
        connection_details['connection_name'] = connection.get('Name', '')
        
        return connection_details
        
    except Exception as e:
        if isinstance(e, JDBCConnectionError):
            raise
        raise JDBCConnectionError(f"Error extracting JDBC details: {str(e)}")


def parse_jdbc_url(jdbc_url: str) -> Dict[str, Any]:
    """
    Parse different types of JDBC URLs to extract connection details.
    
    Args:
        jdbc_url (str): JDBC connection URL
        
    Returns:
        dict: Dictionary with connection details (connection_type, host, port, database)
    """
    # Extract connection type and URL part
    jdbc_parts = jdbc_url.split(':', 2)
    if len(jdbc_parts) < 3:
        raise JDBCConnectionError(f"Invalid JDBC URL format: {jdbc_url}")
    
    connection_type = jdbc_parts[1].lower()
    url_part = jdbc_parts[2]
    
    # Remove leading slashes
    while url_part.startswith('/'):
        url_part = url_part[1:]
    
    result = {
        'connection_type': connection_type,
        'host': None,
        'port': None,
        'database': None,
        'additional_params': {}
    }
    
    # Parse based on connection type
    if connection_type == 'postgresql' or connection_type == 'redshift':
        # Format: jdbc:postgresql://host:port/database or jdbc:redshift://host:port/database
        parsed_url = urlparse(f"http://{url_part}")
        result['host'] = parsed_url.hostname
        result['port'] = parsed_url.port or (5432 if connection_type == 'postgresql' else 5439)
        result['database'] = parsed_url.path.strip('/')
        
        # Parse query parameters
        if parsed_url.query:
            result['additional_params'] = parse_qs(parsed_url.query)
            
    elif connection_type == 'sqlserver' or connection_type == 'microsoft:sqlserver':
        # Format: jdbc:sqlserver://host:port;databaseName=database;property=value
        if connection_type == 'microsoft:sqlserver':
            result['connection_type'] = 'sqlserver'  # Normalize the type
            
        # Split host:port from properties
        if ';' in url_part:
            server_part, properties = url_part.split(';', 1)
        else:
            server_part, properties = url_part, ""
            
        # Parse host and port
        parsed_server = urlparse(f"http://{server_part}")
        result['host'] = parsed_server.hostname
        result['port'] = parsed_server.port or 1433  # Default SQL Server port
        
        # Parse properties (databaseName, etc.)
        properties_dict = {}
        for prop in properties.split(';'):
            if prop and '=' in prop:
                key, value = prop.split('=', 1)
                properties_dict[key.strip()] = value.strip()
                
        # Extract database name
        result['database'] = properties_dict.get('databaseName', '')
        result['additional_params'] = properties_dict
        
    else:
        raise JDBCConnectionError(f"Unsupported database type: {connection_type}")
    
    return result


def get_sqlalchemy_url(conn_details: Dict[str, Any]) -> str:
    """
    Generate a SQLAlchemy connection URL from connection details.
    
    Args:
        conn_details (dict): Connection details dictionary
        
    Returns:
        str: SQLAlchemy connection URL
    """
    conn_type = conn_details.get('connection_type', '')
    host = conn_details.get('host', '')
    port = conn_details.get('port', '')
    database = conn_details.get('database', '')
    user = conn_details.get('user', '')
    password = conn_details.get('password', '')
    
    # Quote the password for URL safety
    quoted_password = quote_plus(password) if password else ""
    
    # Handle different database types
    if conn_type == 'postgresql':
        return f"postgresql://{user}:{quoted_password}@{host}:{port}/{database}"
        
    elif conn_type == 'redshift':
        # Redshift uses PostgreSQL dialect
        return f"postgresql+psycopg2://{user}:{quoted_password}@{host}:{port}/{database}"
        
    elif conn_type == 'sqlserver':
        # For SQL Server, use pyodbc with appropriate driver
        # First, build the base connection string
        conn_str = f"mssql+pyodbc://{user}:{quoted_password}@{host}:{port}/{database}"
        
        # Add driver information
        additional_params = conn_details.get('additional_params', {})
        driver = additional_params.get('driver', 'ODBC Driver 17 for SQL Server')
        driver_quoted = quote_plus(driver)
        conn_str += f"?driver={driver_quoted}"
                
        return conn_str
        
    else:
        raise JDBCConnectionError(f"Unsupported database type: {conn_type}")


def get_engine_from_glue(connection_name: str, region_name: Optional[str] = None):
    """
    Create a SQLAlchemy engine from a Glue connection name.
    
    Args:
        connection_name (str): Glue connection name
        region_name (str, optional): AWS region name
        
    Returns:
        Engine: SQLAlchemy engine
    """
    conn_details = extract_jdbc_from_glue(connection_name, region_name)
    conn_url = get_sqlalchemy_url(conn_details)
    return create_engine(conn_url)


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
        if 'postgresql' in conn_url or 'redshift' in conn_url:
            # PostgreSQL and Redshift system schemas typically start with pg_, information_schema, etc.
            schemas = [s for s in schemas if not s.startswith(('pg_', 'information_schema'))]
        elif 'mssql' in conn_url:
            # SQL Server system schemas
            system_schemas = ['sys', 'INFORMATION_SCHEMA', 'db_accessadmin', 'db_backupoperator', 
                             'db_datareader', 'db_datawriter', 'db_ddladmin', 'db_denydatareader',
                             'db_denydatawriter', 'db_owner', 'db_securityadmin', 'guest']
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
        if 'postgresql' in conn_url or 'redshift' in conn_url:
            schema = 'public'
        elif 'mssql' in conn_url:
            schema = 'dbo'
    
    if exclude_views:
        # Get only tables
        return inspector.get_table_names(schema)
    else:
        # Get tables and views
        tables = inspector.get_table_names(schema)
        views = inspector.get_view_names(schema)
        return tables + views


def get_table_schema(engine, table_name: str, schema: Optional[str] = None) -> List[Dict[str, Any]]:
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


def get_table_sample(engine, table_name: str, schema: Optional[str] = None, limit: int = 10) -> pl.DataFrame:
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


def read_table(engine, table_name: str, schema: Optional[str] = None, 
               columns: Optional[List[str]] = None, filters: Optional[str] = None,
               batch_size: int = 10000) -> pl.DataFrame:
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


def get_table_stats(engine, table_name: str, schema: Optional[str] = None) -> Dict[str, Any]:
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
        if 'postgresql' in conn_url:
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
                        'total_size': result[0],
                        'table_size': result[1],
                        'index_size': result[2]
                    }
                    
        elif 'redshift' in conn_url:
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
                    size_info = {'size_mb': result[1]}
    except:
        # If size query fails, continue without size info
        pass
    
    # Return combined stats
    return {
        'table_name': table_name,
        'schema': schema,
        'row_count': row_count,
        'column_count': len(columns),
        'columns': columns,
        'size_info': size_info
    }


def read_sql_with_polars(connection_name: str, query: str, region_name: Optional[str] = None, 
                         batch_size: int = 10000) -> pl.DataFrame:
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
    engine = get_engine_from_glue(connection_name, region_name)
    return pl.read_database(query=query, connection=engine, batch_size=batch_size)