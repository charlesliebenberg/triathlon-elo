"""
Generate Data Package

This package contains modules for collecting, analyzing, and uploading triathlon data.
"""

# Make sure the data directory exists
import os
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
os.makedirs(DATA_DIR, exist_ok=True) 