#!/bin/bash

# Create Python virtual environment
python3 -m venv venv

# Activate virtual environment
. venv/bin/activate

# Install dependencies
pip install --upgrade pip
pip install -r requirements.txt

# Run table verification
python src/data_warehouse/verify_tables.py 