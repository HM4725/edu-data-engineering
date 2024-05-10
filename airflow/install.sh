#!/bin/bash

AIRFLOW_VERSION=2.9.1

# Extract the version of Python you have installed. If you're currently using a Python version that is not supported by Airflow, you may want to set this manually.
# See above for supported versions.
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
# For example this would install 2.9.1 with python 3.8: https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.8.txt

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# Set AIRFLOW_HOME
RELATIVE_DIR=`dirname "$0"`
CANONICAL_DIR=`readlink -f $RELATIVE_DIR`
echo "" >> ~/.bashrc
echo "# Generated by edu-data-engineering-airflow" >> ~/.bashrc
echo "export AIRFLOW_HOME=$CANONICAL_DIR" >> ~/.bashrc
