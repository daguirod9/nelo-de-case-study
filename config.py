"""
Configuration settings for the Medallion Architecture Analytics Pipeline.

This module defines configuration for:
- AWS/SQS settings
- DuckDB paths
- Medallion layer paths (Bronze, Silver, Gold, Platinum)
- Processing parameters
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# =============================================================================
# AWS CONFIGURATION
# =============================================================================
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")

# =============================================================================
# SQS CONFIGURATION
# =============================================================================
SQS_QUEUE_NAME = "data-engineering-case-analytics-queue"
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL")

# Consumer settings
MAX_MESSAGES_PER_BATCH = 20  # SQS maximum is 10
VISIBILITY_TIMEOUT = 30  # seconds
WAIT_TIME_SECONDS = 20  # Long polling (max 20 seconds)
POLLING_INTERVAL = 5  # seconds between polling cycles

# =============================================================================
# MEDALLION ARCHITECTURE PATHS
# =============================================================================

# Base data directory
DATA_PATH = os.getenv("DATA_PATH", "data")

# Bronze Layer - Raw JSON storage
BRONZE_PATH = os.getenv("BRONZE_PATH", f"{DATA_PATH}/bronze")

# Silver Layer - Parsed and cleaned data
SILVER_PATH = os.getenv("SILVER_PATH", f"{DATA_PATH}/silver")

# Gold Layer - Dimensional models and fact tables
GOLD_PATH = os.getenv("GOLD_PATH", f"{DATA_PATH}/gold")

# Platinum Layer - Aggregated metrics
PLATINUM_PATH = os.getenv("PLATINUM_PATH", f"{DATA_PATH}/platinum")

# DuckDB database file (contains Silver, Gold, Platinum schemas)
DUCKDB_PATH = os.getenv("DUCKDB_PATH", f"{DATA_PATH}/analytics.duckdb")

# =============================================================================
# PARQUET SETTINGS
# =============================================================================
SAVE_PARQUET = os.getenv("SAVE_PARQUET", "true").lower() == "true"
PARQUET_COMPRESSION = os.getenv("PARQUET_COMPRESSION", "snappy")

# =============================================================================
# PROCESSING CONFIGURATION
# =============================================================================
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))  # Number of messages to accumulate before processing
PROCESSING_INTERVAL = int(os.getenv("PROCESSING_INTERVAL", "60"))  # seconds between batch processing

# Session timeout for Gold layer session derivation
SESSION_TIMEOUT_MINUTES = int(os.getenv("SESSION_TIMEOUT_MINUTES", "30"))

# =============================================================================
# PLATINUM METRICS COMPUTATION
# =============================================================================
# How often to compute Platinum metrics (in minutes)
PLATINUM_COMPUTATION_INTERVAL = int(os.getenv("PLATINUM_COMPUTATION_INTERVAL", "60"))

# Days of data to include in metric computations
PLATINUM_LOOKBACK_DAYS = int(os.getenv("PLATINUM_LOOKBACK_DAYS", "7"))

# =============================================================================
# LOGGING
# =============================================================================
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE = os.getenv("LOG_FILE", "pipeline.log")

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def ensure_directories():
    """
    Ensure all required directories exist.
    
    Creates the directory structure for the Medallion architecture:
    - data/bronze/
    - data/silver/
    - data/gold/
    - data/platinum/
    """
    paths = [BRONZE_PATH, SILVER_PATH, GOLD_PATH, PLATINUM_PATH]
    for path in paths:
        Path(path).mkdir(parents=True, exist_ok=True)


def get_layer_path(layer: str) -> str:
    """
    Get the path for a specific layer.
    
    Args:
        layer: Layer name ('bronze', 'silver', 'gold', 'platinum').
        
    Returns:
        Path string for the specified layer.
        
    Raises:
        ValueError: If an unknown layer is specified.
    """
    layer_map = {
        "bronze": BRONZE_PATH,
        "silver": SILVER_PATH,
        "gold": GOLD_PATH,
        "platinum": PLATINUM_PATH,
    }
    
    if layer not in layer_map:
        raise ValueError(f"Unknown layer: {layer}. Valid options: {list(layer_map.keys())}")
    
    return layer_map[layer]
