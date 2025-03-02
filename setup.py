from setuptools import setup, find_packages

setup(
    name="polars_jdbc_tools",
    version="0.1.0",
    description="A comprehensive toolkit for working with JDBC connections in AWS environments using Polars",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="mfilipelino",
    url="https://github.com/mfilipelino/pjt",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[
        "boto3>=1.28.0",
        "polars>=0.18.0",
        "sqlalchemy>=2.0.0",
    ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)