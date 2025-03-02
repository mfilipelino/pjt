"""
Unit tests for core JDBC connection utilities.
"""

import pytest

from polars_jdbc_tools.core import JDBCConnectionError, get_sqlalchemy_url, parse_jdbc_url


class TestParseJdbcUrl:
    def test_postgresql_url(self):
        url = "jdbc:postgresql://myhost.example.com:5432/mydb"
        result = parse_jdbc_url(url)

        assert result["connection_type"] == "postgresql"
        assert result["host"] == "myhost.example.com"
        assert result["port"] == 5432
        assert result["database"] == "mydb"

    def test_postgresql_url_default_port(self):
        url = "jdbc:postgresql://myhost.example.com/mydb"
        result = parse_jdbc_url(url)

        assert result["connection_type"] == "postgresql"
        assert result["host"] == "myhost.example.com"
        assert result["port"] == 5432  # Default PostgreSQL port
        assert result["database"] == "mydb"

    def test_redshift_url(self):
        url = "jdbc:redshift://myhost.example.com:5439/mydb"
        result = parse_jdbc_url(url)

        assert result["connection_type"] == "redshift"
        assert result["host"] == "myhost.example.com"
        assert result["port"] == 5439
        assert result["database"] == "mydb"

    def test_sqlserver_url(self):
        url = "jdbc:sqlserver://myhost.example.com:1433;databaseName=mydb;encrypt=true"
        result = parse_jdbc_url(url)

        assert result["connection_type"] == "sqlserver"
        assert result["host"] == "myhost.example.com"
        assert result["port"] == 1433
        assert result["database"] == "mydb"
        assert result["additional_params"]["encrypt"] == "true"

    def test_microsoft_sqlserver_url(self):
        url = "jdbc:sqlserver://myhost.example.com;databaseName=mydb"
        result = parse_jdbc_url(url)

        assert result["connection_type"] == "sqlserver"  # Normalized
        assert result["host"] == "myhost.example.com"
        assert result["port"] == 1433  # Default SQL Server port
        assert result["database"] == "mydb"

    def test_invalid_url_format(self):
        url = "invalid:url"
        with pytest.raises(JDBCConnectionError):
            parse_jdbc_url(url)

    def test_unsupported_database_type(self):
        url = "jdbc:oracle://myhost.example.com:1521/mydb"
        with pytest.raises(JDBCConnectionError):
            parse_jdbc_url(url)


class TestGetSqlalchemyUrl:
    def test_postgresql_url(self):
        conn_details = {
            "connection_type": "postgresql",
            "host": "myhost.example.com",
            "port": 5432,
            "database": "mydb",
            "user": "user",
            "password": "pass"
        }

        url = get_sqlalchemy_url(conn_details)
        assert url == "postgresql://user:pass@myhost.example.com:5432/mydb"

    def test_redshift_url(self):
        conn_details = {
            "connection_type": "redshift",
            "host": "myhost.example.com",
            "port": 5439,
            "database": "mydb",
            "user": "user",
            "password": "pass"
        }

        url = get_sqlalchemy_url(conn_details)
        assert url == "postgresql+psycopg2://user:pass@myhost.example.com:5439/mydb"

    def test_sqlserver_url(self):
        conn_details = {
            "connection_type": "sqlserver",
            "host": "myhost.example.com",
            "port": 1433,
            "database": "mydb",
            "user": "user",
            "password": "pass",
            "additional_params": {
                "driver": "ODBC Driver 17 for SQL Server"
            }
        }

        url = get_sqlalchemy_url(conn_details)
        expected = "mssql+pyodbc://user:pass@myhost.example.com:1433/mydb?driver=ODBC+Driver+17+for+SQL+Server"
        assert url == expected

    def test_password_with_special_chars(self):
        conn_details = {
            "connection_type": "postgresql",
            "host": "myhost.example.com",
            "port": 5432,
            "database": "mydb",
            "user": "user",
            "password": "p@ss:w0rd!"
        }

        url = get_sqlalchemy_url(conn_details)
        assert "p%40ss%3Aw0rd%21" in url  # Check that special chars are URL-encoded

    def test_unsupported_database_type(self):
        conn_details = {
            "connection_type": "oracle",
            "host": "myhost.example.com",
            "port": 1521,
            "database": "mydb",
            "user": "user",
            "password": "pass"
        }

        with pytest.raises(JDBCConnectionError):
            get_sqlalchemy_url(conn_details)
