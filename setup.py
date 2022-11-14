import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()
    
install_reqs = [
  'snowflake-connector-python==2.7.9',
  'typing-extensions==4.3.0'
]

setuptools.setup(
    name="snowflake-stream-reader",
    version="0.0.1",
    install_requires=install_reqs,
    author="Ryan Chynoweth",
    author_email="ryan.chynoweth@databricks.com",
    description="A package that can be used to read Snowflake Stream (CDC) data on the Databricks runtime.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/rchynoweth/SnowflakeStreamReader",
    packages=setuptools.find_packages(),
    classifiers=[],
)