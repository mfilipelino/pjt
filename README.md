# Polars JDBC Tools - Function Reference

A comprehensive toolkit for working with JDBC connections in AWS environments using Polars.

## Connection Management

### list_glue_connections
Lists all AWS Glue connection names in a region.

```python
import polars_jdbc_tools as pjt

# List connections in the default region
connections = pjt.list_glue_connections()
print(connections)

# List connections in a specific region
us_west_connections = pjt.list_glue_connections(region_name="us-west-2")
```

### extract_jdbc_from_glue
Extracts JDBC connection details from an AWS Glue Connection.

```python
# Get connection details
conn_details = pjt.extract_jdbc_from_glue("my-postgres-connection")
print(f"Host: {conn_details['host']}")
print(f"Database: {conn_details['database']}")
print(f"Connection type: {conn_details['connection_type']}")
```

### parse_jdbc_url
Parses different types of JDBC URLs to extract connection details.

```python
# Parse a JDBC URL directly
jdbc_url = "jdbc:postgresql://myhost.example.com:5432/mydb"
conn_info = pjt.parse_jdbc_url(jdbc_url)
print(conn_info)
```

### get_sqlalchemy_url
Generates a SQLAlchemy connection URL from connection details.

```python
# Get SQLAlchemy URL from connection details
conn_details = pjt.extract_jdbc_from_glue("my-redshift-connection")
sqlalchemy_url = pjt.get_sqlalchemy_url(conn_details)
print(sqlalchemy_url)
```

### get_engine_from_glue
Creates a SQLAlchemy engine directly from a Glue connection name.

```python
# Create an engine from a Glue connection
engine = pjt.get_engine_from_glue("my-sqlserver-connection")

# Use the engine with SQLAlchemy
with engine.connect() as conn:
    result = conn.execute("SELECT TOP 5 * FROM customers")
    for row in result:
        print(row)
```

## Database Exploration

### list_schemas
Lists all schemas in a database.

```python
# List all non-system schemas
engine = pjt.get_engine_from_glue("my-postgres-connection")
schemas = pjt.list_schemas(engine)
print(schemas)

# Include system schemas
all_schemas = pjt.list_schemas(engine, exclude_system=False)
```

### list_tables
Lists all tables in a schema.

```python
# List tables in the default schema
engine = pjt.get_engine_from_glue("my-redshift-connection")
tables = pjt.list_tables(engine)
print(tables)

# List tables in a specific schema, excluding views
tables_only = pjt.list_tables(engine, schema="analytics", exclude_views=True)
```

### get_table_schema
Gets schema information for a table.

```python
# Get column details for a table
engine = pjt.get_engine_from_glue("my-postgres-connection")
columns = pjt.get_table_schema(engine, "customers", schema="public")

# Print column names and types
for col in columns:
    print(f"{col['name']}: {col['type']}")
```

### get_table_stats
Gets basic statistics about a table.

```python
# Get table statistics
engine = pjt.get_engine_from_glue("my-postgres-connection")
stats = pjt.get_table_stats(engine, "orders", schema="sales")

print(f"Row count: {stats['row_count']}")
print(f"Column count: {stats['column_count']}")
print(f"Total size: {stats['size_info'].get('total_size', 'unknown')}")
```

## Data Access

### get_table_sample
Gets a sample of data from a table using Polars.

```python
# Get a sample of 5 rows
engine = pjt.get_engine_from_glue("my-redshift-connection")
sample_df = pjt.get_table_sample(engine, "customers", limit=5)
print(sample_df)
```

### read_table
Reads a table into a Polars DataFrame with filtering options.

```python
# Read specific columns with a filter
engine = pjt.get_engine_from_glue("my-postgres-connection")
df = pjt.read_table(
    engine,
    "orders",
    schema="sales",
    columns=["order_id", "customer_id", "amount", "order_date"],
    filters="order_date >= '2023-01-01' AND amount > 100",
    batch_size=50000
)

# Process the data with Polars
monthly_sales = df.group_by(pl.col("order_date").dt.month()).agg(
    pl.sum("amount").alias("monthly_total")
)
```

### execute_query
Executes a SQL query and returns results as a Polars DataFrame.

```python
# Run a custom SQL query
engine = pjt.get_engine_from_glue("my-sqlserver-connection")
query = """
SELECT 
    c.customer_name, 
    SUM(o.amount) as total_spend
FROM 
    sales.orders o
JOIN 
    dbo.customers c ON o.customer_id = c.id
WHERE 
    o.order_date BETWEEN '2023-01-01' AND '2023-12-31'
GROUP BY 
    c.customer_name
ORDER BY 
    total_spend DESC
"""
results_df = pjt.execute_query(engine, query)
print(results_df.head(10))
```

### read_sql_with_polars
Convenience function to read SQL directly using a Glue connection name.

```python
# One-liner to execute SQL and get a Polars DataFrame
top_products_df = pjt.read_sql_with_polars(
    "my-redshift-connection",
    """
    SELECT 
        product_id, 
        product_name, 
        SUM(quantity) as units_sold 
    FROM 
        sales.order_items oi
    JOIN 
        products p ON oi.product_id = p.id
    GROUP BY 
        product_id, product_name
    ORDER BY 
        units_sold DESC
    LIMIT 10
    """
)

# Plot the results
import matplotlib.pyplot as plt

plt.figure(figsize=(10, 6))
plt.bar(top_products_df["product_name"], top_products_df["units_sold"])
plt.title("Top 10 Products by Units Sold")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.show()
```

## Error Handling

All functions raise `JDBCConnectionError` when there's an issue with connections or SQL execution. You can catch these exceptions to provide better error handling:

```python
try:
    engine = pjt.get_engine_from_glue("non-existent-connection")
except pjt.JDBCConnectionError as e:
    print(f"Connection error: {e}")
    # Handle the error appropriately
```

## Complete Example Workflow

Here's a complete workflow example:

```python
import polars_jdbc_tools as pjt
import polars as pl
import matplotlib.pyplot as plt

# 1. Find available connections
connections = pjt.list_glue_connections(region_name="us-east-1")
print(f"Available connections: {connections}")

# 2. Create an engine
engine = pjt.get_engine_from_glue("sales-database")

# 3. Explore the database
schemas = pjt.list_schemas(engine)
print(f"Schemas: {schemas}")

tables = pjt.list_tables(engine, schema="sales")
print(f"Tables in sales schema: {tables}")

# 4. Check table structure and stats
customer_schema = pjt.get_table_schema(engine, "customers", schema="sales")
print("Customer table columns:")
for col in customer_schema:
    print(f"  - {col['name']} ({col['type']})")

stats = pjt.get_table_stats(engine, "orders", schema="sales")
print(f"Orders table has {stats['row_count']} rows")

# 5. Load and analyze data
orders_df = pjt.read_table(
    engine,
    "orders",
    schema="sales",
    filters="order_date >= '2023-01-01'"
)

# 6. Perform analysis with Polars
monthly_revenue = orders_df.group_by(
    pl.col("order_date").dt.month().alias("month")
).agg(
    pl.sum("amount").alias("revenue"),
    pl.count().alias("order_count")
)

# 7. Visualize the results
plt.figure(figsize=(12, 6))
plt.bar(monthly_revenue["month"], monthly_revenue["revenue"])
plt.title("Monthly Revenue (2023)")
plt.xlabel("Month")
plt.ylabel("Revenue")
plt.xticks(range(1, 13))
plt.grid(axis="y", linestyle="--", alpha=0.7)
plt.show()
```