from setuptools import setup, find_packages

setup(
    name="polars_jdbc_tools",
    version="0.1.0",
    description="A comprehensive toolkit for working with JDBC connections in AWS environments using Polars",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[
        "boto3>=1.28.0",
        "polars>=0.18.0",
        "sqlalchemy>=2.0.0",
    ],
)