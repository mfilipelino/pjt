"""
AWS-related utilities for JDBC connections.

This module provides functions to interact with AWS services, particularly
for retrieving connection details from AWS Glue connections.
"""

from typing import Any, Dict, List, Optional

import boto3
from sqlalchemy import create_engine

from .core import JDBCConnectionError, get_sqlalchemy_url, parse_jdbc_url


def list_glue_connections(region_name=None) -> List[str]:
    """
    List all AWS Glue connection names in a region.

    Args:
        region_name (str, optional): AWS region name. If None, uses default from AWS config.

    Returns:
        List[str]: List of connection names

    Raises:
        JDBCConnectionError: If there's an error communicating with AWS Glue
    """
    try:
        # Initialize Glue client
        glue_client = boto3.client("glue", region_name=region_name)

        # Get list of connections (paginated results)
        connection_names = []
        paginator = glue_client.get_paginator("get_connections")

        for page in paginator.paginate():
            # Extract just the names from each connection
            names = [conn.get("Name") for conn in page.get("ConnectionList", [])]
            connection_names.extend(names)

        return connection_names

    except Exception as e:
        raise JDBCConnectionError(f"Error listing connections: {str(e)}")


def extract_jdbc_from_glue(
    connection_name: str, region_name: Optional[str] = None
) -> Dict[str, Any]:
    """
    Extract JDBC connection details from an AWS Glue Connection.

    Args:
        connection_name (str): Name of the Glue connection
        region_name (str, optional): AWS region name. If None, uses default from AWS config.

    Returns:
        dict: Dictionary containing connection details
              (connection_type, host, port, database, user, password)

    Raises:
        JDBCConnectionError: If the connection doesn't exist, is not a JDBC connection,
                             or there's an error communicating with AWS Glue
    """
    try:
        # Initialize Glue client
        glue_client = boto3.client("glue", region_name=region_name)

        # Get connection details
        response = glue_client.get_connection(Name=connection_name)

        # Check if connection exists
        if "Connection" not in response:
            raise JDBCConnectionError(f"Connection '{connection_name}' not found")

        connection = response["Connection"]
        connection_properties = connection.get("ConnectionProperties", {})

        # Check if it's a JDBC connection
        if "JDBC_CONNECTION_URL" not in connection_properties:
            raise JDBCConnectionError(
                f"Connection '{connection_name}' is not a JDBC connection"
            )

        # Extract JDBC URL
        jdbc_url = connection_properties["JDBC_CONNECTION_URL"]

        # Check if it's a valid JDBC URL
        if not jdbc_url.startswith("jdbc:"):
            raise JDBCConnectionError(f"Invalid JDBC URL format: {jdbc_url}")

        # Get username and password
        username = connection_properties.get("USERNAME", "")
        password = connection_properties.get("PASSWORD", "")

        # Parse connection details based on database type
        connection_details = parse_jdbc_url(jdbc_url)
        connection_details["user"] = username
        connection_details["password"] = password
        connection_details["jdbc_url"] = jdbc_url
        connection_details["connection_name"] = connection.get("Name", "")

        return connection_details

    except Exception as e:
        if isinstance(e, JDBCConnectionError):
            raise
        raise JDBCConnectionError(f"Error extracting JDBC details: {str(e)}")


def get_engine_from_glue(connection_name: str, region_name: Optional[str] = None):
    """
    Create a SQLAlchemy engine from a Glue connection name.

    Args:
        connection_name (str): Glue connection name
        region_name (str, optional): AWS region name

    Returns:
        Engine: SQLAlchemy engine

    Raises:
        JDBCConnectionError: If there's an error getting the connection details or creating the engine
    """
    conn_details = extract_jdbc_from_glue(connection_name, region_name)
    conn_url = get_sqlalchemy_url(conn_details)
    return create_engine(conn_url)
