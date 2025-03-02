"""
Core JDBC connection utilities without external dependencies.

This module provides pure functions for parsing and generating JDBC and SQLAlchemy connection URLs
without any dependencies on external services or libraries (except standard library).
"""

from typing import Any, Dict
from urllib.parse import parse_qs, quote_plus, urlparse


class JDBCConnectionError(Exception):
    """Exception raised for errors in JDBC connections."""

    pass


def parse_jdbc_url(jdbc_url: str) -> Dict[str, Any]:
    """
    Parse different types of JDBC URLs to extract connection details.

    Args:
        jdbc_url (str): JDBC connection URL

    Returns:
        dict: Dictionary with connection details (connection_type, host, port, database)

    Raises:
        JDBCConnectionError: If the URL format is invalid or the database type is unsupported
    """
    # Extract connection type and URL part
    jdbc_parts = jdbc_url.split(":", 2)
    if len(jdbc_parts) < 3:
        raise JDBCConnectionError(f"Invalid JDBC URL format: {jdbc_url}")

    connection_type = jdbc_parts[1].lower()
    url_part = jdbc_parts[2]

    # Remove leading slashes
    while url_part.startswith("/"):
        url_part = url_part[1:]

    result = {
        "connection_type": connection_type,
        "host": None,
        "port": None,
        "database": None,
        "additional_params": {},
    }

    # Parse based on connection type
    if connection_type == "postgresql" or connection_type == "redshift":
        # Format: jdbc:postgresql://host:port/database or jdbc:redshift://host:port/database
        parsed_url = urlparse(f"http://{url_part}")
        result["host"] = parsed_url.hostname
        result["port"] = parsed_url.port or (
            5432 if connection_type == "postgresql" else 5439
        )
        result["database"] = parsed_url.path.strip("/")

        # Parse query parameters
        if parsed_url.query:
            result["additional_params"] = parse_qs(parsed_url.query)

    elif connection_type == "sqlserver" or connection_type == "microsoft:sqlserver":
        # Format: jdbc:sqlserver://host:port;databaseName=database;property=value
        if connection_type == "microsoft:sqlserver":
            result["connection_type"] = "sqlserver"  # Normalize the type

        # Split host:port from properties
        if ";" in url_part:
            server_part, properties = url_part.split(";", 1)
        else:
            server_part, properties = url_part, ""

        # Parse host and port
        parsed_server = urlparse(f"http://{server_part}")
        result["host"] = parsed_server.hostname
        result["port"] = parsed_server.port or 1433  # Default SQL Server port

        # Parse properties (databaseName, etc.)
        properties_dict = {}
        for prop in properties.split(";"):
            if prop and "=" in prop:
                key, value = prop.split("=", 1)
                properties_dict[key.strip()] = value.strip()

        # Extract database name
        result["database"] = properties_dict.get("databaseName", "")
        result["additional_params"] = properties_dict

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

    Raises:
        JDBCConnectionError: If the database type is unsupported
    """
    conn_type = conn_details.get("connection_type", "")
    host = conn_details.get("host", "")
    port = conn_details.get("port", "")
    database = conn_details.get("database", "")
    user = conn_details.get("user", "")
    password = conn_details.get("password", "")

    # Quote the password for URL safety
    quoted_password = quote_plus(password) if password else ""

    # Handle different database types
    if conn_type == "postgresql":
        return f"postgresql://{user}:{quoted_password}@{host}:{port}/{database}"

    elif conn_type == "redshift":
        # Redshift uses PostgreSQL dialect
        return (
            f"postgresql+psycopg2://{user}:{quoted_password}@{host}:{port}/{database}"
        )

    elif conn_type == "sqlserver":
        # For SQL Server, use pyodbc with appropriate driver
        # First, build the base connection string
        conn_str = f"mssql+pyodbc://{user}:{quoted_password}@{host}:{port}/{database}"

        # Add driver information
        additional_params = conn_details.get("additional_params", {})
        driver = additional_params.get("driver", "ODBC Driver 17 for SQL Server")
        driver_quoted = quote_plus(driver)
        conn_str += f"?driver={driver_quoted}"

        return conn_str

    else:
        raise JDBCConnectionError(f"Unsupported database type: {conn_type}")
