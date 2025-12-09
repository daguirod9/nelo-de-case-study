"""
Loader module for the Medallion Architecture Analytics Pipeline.

This module provides:
- DuckDBLoader: Generic class for DuckDB operations and Parquet export
- Transform functions: Specific transformations for Silver and Gold layers
"""
import logging
from pathlib import Path
from typing import Optional
from datetime import datetime

import duckdb
import pandas as pd

import config
from src.models import SILVER_SCHEMAS, GOLD_SCHEMAS

logger = logging.getLogger(__name__)

SQL_MODELS_PATH = Path("sql/models")


class DuckDBLoader:
    """
    Generic DuckDB loader for SQL execution and Parquet export.
    
    This class provides low-level operations:
    - Connection management
    - Schema initialization
    - SQL execution (queries, files, models)
    - Parquet export
    """

    def __init__(
        self,
        db_path: Optional[str] = None,
        data_path: str = "data",
    ):
        """
        Initialize the DuckDB loader.
        
        Args:
            db_path: Path to the DuckDB database file.
            data_path: Base path for data storage.
        """
        self.db_path = db_path or config.DUCKDB_PATH
        self.data_path = Path(data_path)
        
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        
        self.bronze_path = self.data_path / "bronze"
        self.silver_path = self.data_path / "silver"
        self.gold_path = self.data_path / "gold"
        
        for path in [self.bronze_path, self.silver_path, self.gold_path]:
            path.mkdir(parents=True, exist_ok=True)
        
        self.connection: Optional[duckdb.DuckDBPyConnection] = None
        logger.info(f"DuckDBLoader initialized - DB: {self.db_path}")

    def connect(self) -> duckdb.DuckDBPyConnection:
        """Establish connection to DuckDB."""
        if self.connection is None:
            self.connection = duckdb.connect(self.db_path)
            logger.info(f"Connected to DuckDB at {self.db_path}")
        return self.connection

    def close(self):
        """Close the DuckDB connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("DuckDB connection closed")

    def query(self, sql: str) -> pd.DataFrame:
        """Execute a SQL query and return results as DataFrame."""
        return self.connect().execute(sql).fetchdf()

    def execute(self, sql: str):
        """Execute a single SQL statement."""
        self.connect().execute(sql)

    def execute_statements(self, sql: str):
        """Execute multiple SQL statements separated by semicolons."""
        conn = self.connect()
        
        # Remove SQL comments (lines starting with --)
        lines = sql.split('\n')
        cleaned_lines = []
        for line in lines:
            stripped = line.strip()
            if not stripped.startswith('--'):
                cleaned_lines.append(line)
        sql_clean = '\n'.join(cleaned_lines)
        
        # Split by semicolons but handle edge cases
        statements = []
        current_stmt = []
        
        for line in sql_clean.split('\n'):
            current_stmt.append(line)
            # Check if line ends with semicolon (end of statement)
            if line.strip().endswith(';'):
                full_stmt = '\n'.join(current_stmt).strip()
                if full_stmt and full_stmt != ';':
                    # Remove trailing semicolon for execution
                    statements.append(full_stmt.rstrip(';').strip())
                current_stmt = []
        
        # Handle any remaining statement without semicolon
        if current_stmt:
            full_stmt = '\n'.join(current_stmt).strip()
            if full_stmt:
                statements.append(full_stmt)
        
        for i, statement in enumerate(statements):
            if statement:
                try:
                    logger.debug(f"Executing statement {i+1}/{len(statements)}: {statement[:100]}...")
                    conn.execute(statement)
                except Exception as e:
                    logger.error(f"Error executing SQL statement {i+1}: {e}")
                    logger.error(f"Statement: {statement[:500]}...")
                    raise

    def execute_file(self, sql_file_path: str):
        """Execute SQL statements from a file."""
        with open(sql_file_path, "r") as f:
            self.execute_statements(f.read())
        logger.info(f"Executed SQL file: {sql_file_path}")

    def run_model(self, model_name: str, **kwargs):
        """
        Execute a SQL model file from sql/models/ directory.
        
        Args:
            model_name: Name of the SQL model (without .sql extension).
            **kwargs: Template variables to substitute (e.g., bronze_path).
        """
        sql_path = SQL_MODELS_PATH / f"{model_name}.sql"
        
        if not sql_path.exists():
            raise FileNotFoundError(f"SQL model not found: {sql_path}")
        
        with open(sql_path, "r") as f:
            sql = f.read()
        
        # Simple template rendering
        for key, value in kwargs.items():
            placeholder = f"{{{{ {key} }}}}"
            if placeholder in sql:
                sql = sql.replace(placeholder, str(value))
                logger.debug(f"Replaced template '{placeholder}' with '{value}'")
            else:
                logger.warning(f"Template placeholder '{placeholder}' not found in SQL model {model_name}")
        
        # Check for unreplaced placeholders
        import re
        remaining = re.findall(r'\{\{.*?\}\}', sql)
        if remaining:
            logger.warning(f"Unreplaced placeholders in {model_name}: {remaining}")
        
        self.execute_statements(sql)
        logger.debug(f"Executed SQL model: {model_name}")

    def export_to_parquet(self, table_name: str, output_dir: Path, partition_col: str = None):
        """
        Export a DuckDB table to Parquet files.
        
        Args:
            table_name: Name of the table to export.
            output_dir: Directory path for Parquet output.
            partition_col: Optional column to partition by.
        """
        conn = self.connect()
        output_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
        
        if partition_col:
            try:
                conn.execute(f"""
                    COPY (SELECT * FROM {table_name}) 
                    TO '{output_dir}' 
                    (FORMAT PARQUET, PARTITION_BY ({partition_col}))
                """)
            except Exception as e:
                logger.warning(f"Partitioned export failed: {e}")
                file_path = output_dir / f"data_{timestamp}.parquet"
                conn.execute(f"COPY (SELECT * FROM {table_name}) TO '{file_path}' (FORMAT PARQUET)")
        else:
            file_path = output_dir / f"data_{timestamp}.parquet"
            conn.execute(f"COPY (SELECT * FROM {table_name}) TO '{file_path}' (FORMAT PARQUET)")
        
        logger.debug(f"Exported {table_name} to Parquet: {output_dir}")

    def read_parquet(self, parquet_path: Path) -> pd.DataFrame:
        """Read Parquet files from a directory."""
        if not parquet_path.exists():
            logger.warning(f"Path does not exist: {parquet_path}")
            return pd.DataFrame()
        
        try:
            return self.connect().execute(
                f"SELECT * FROM read_parquet('{parquet_path}/**/*.parquet')"
            ).fetchdf()
        except Exception as e:
            logger.warning(f"Error reading parquet: {e}")
            return pd.DataFrame()

    def table_count(self, table_name: str) -> int:
        """Get row count for a table."""
        try:
            return self.connect().execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        except:
            return 0

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# =============================================================================
# Schema Initialization
# =============================================================================

def initialize_schemas(loader: DuckDBLoader, layers: list[str] = None):
    """
    Initialize database schemas for specified layers.
    
    Args:
        loader: DuckDBLoader instance.
        layers: List of layers to initialize ('silver', 'gold').
    """
    layers = layers or ["silver", "gold"]
    schema_map = {"silver": SILVER_SCHEMAS, "gold": GOLD_SCHEMAS}
    
    for layer in layers:
        for schema_sql in schema_map.get(layer, []):
            loader.execute_statements(schema_sql)
        logger.debug(f"{layer.capitalize()} schema initialized")
    
    logger.info(f"Schemas initialized for: {layers}")


# =============================================================================
# Layer Transformations
# =============================================================================

def transform_to_silver(loader: DuckDBLoader) -> dict:
    """
    Transform Bronze JSON files to Silver tables using SQL models.
    
    Args:
        loader: DuckDBLoader instance.
        
    Returns:
        Dictionary with transformation statistics.
    """
    bronze_path_str = str(loader.bronze_path)
    logger.info(f"Transforming Bronze to Silver from: {bronze_path_str}")
    
    # Verify bronze files exist
    bronze_files = list(loader.bronze_path.rglob("*.json"))
    logger.info(f"Found {len(bronze_files)} JSON files in Bronze")
    
    if not bronze_files:
        logger.warning("No JSON files found in Bronze layer!")
        return {"events_inserted": 0, "items_inserted": 0, "total_events": 0, "total_items": 0}
    
    # Test if DuckDB can read the JSON files
    try:
        test_count = loader.query(f"""
            SELECT COUNT(*) as cnt 
            FROM read_json_auto('{bronze_path_str}/**/*.json', maximum_object_size=10485760, ignore_errors=true)
        """)
        json_count = test_count['cnt'].iloc[0] if not test_count.empty else 0
        logger.info(f"DuckDB can read {json_count} records from Bronze JSON files")
    except Exception as e:
        logger.error(f"Error testing JSON read: {e}")
        json_count = 0
    
    events_before = loader.table_count("silver_events")
    items_before = loader.table_count("silver_items")
    
    logger.info(f"Before transformation - Events: {events_before}, Items: {items_before}")
    
    try:
        loader.run_model("silver_events", bronze_path=bronze_path_str)
        logger.info("silver_events model executed successfully")
    except Exception as e:
        logger.error(f"Error running silver_events model: {e}")
        raise
    
    try:
        loader.run_model("silver_items", bronze_path=bronze_path_str)
        logger.info("silver_items model executed successfully")
    except Exception as e:
        logger.error(f"Error running silver_items model: {e}")
        raise
    
    events_after = loader.table_count("silver_events")
    items_after = loader.table_count("silver_items")
    
    stats = {
        "events_inserted": events_after - events_before,
        "items_inserted": items_after - items_before,
        "total_events": events_after,
        "total_items": items_after,
    }
    logger.info(f"Silver transformation complete: {stats}")
    return stats


def transform_to_gold(loader: DuckDBLoader) -> dict:
    """
    Transform Silver tables to Gold dimensional model using SQL models.
    
    Args:
        loader: DuckDBLoader instance.
        
    Returns:
        Dictionary with transformation statistics.
    """
    logger.info("Transforming Silver to Gold...")
    
    facts_before = loader.table_count("fact_events")
    items_before = loader.table_count("fact_event_items")
    dim_items_before = loader.table_count("dim_items")
    dim_users_before = loader.table_count("dim_users")
    
    loader.run_model("gold_fact_events")
    loader.run_model("gold_fact_items")
    loader.run_model("gold_dim_items")
    loader.run_model("gold_dim_users")
    
    stats = {
        "fact_events_inserted": loader.table_count("fact_events") - facts_before,
        "fact_items_inserted": loader.table_count("fact_event_items") - items_before,
        "dim_items_inserted": loader.table_count("dim_items") - dim_items_before,
        "dim_users_inserted": loader.table_count("dim_users") - dim_users_before,
    }
    logger.info(f"Gold transformation complete: {stats}")
    return stats


# =============================================================================
# Parquet Export
# =============================================================================

def export_silver_to_parquet(loader: DuckDBLoader):
    """Export all Silver tables to Parquet."""
    loader.export_to_parquet("silver_events", loader.silver_path / "silver_events", "event_timestamp")
    loader.export_to_parquet("silver_items", loader.silver_path / "silver_items")
    logger.info("Silver tables exported to Parquet")


def export_gold_to_parquet(loader: DuckDBLoader):
    """Export all Gold tables to Parquet."""
    loader.export_to_parquet("fact_events", loader.gold_path / "fact_events", "event_date")
    loader.export_to_parquet("fact_event_items", loader.gold_path / "fact_event_items")
    loader.export_to_parquet("dim_items", loader.gold_path / "dim_items")
    loader.export_to_parquet("dim_users", loader.gold_path / "dim_users")
    logger.info("Gold tables exported to Parquet")


# =============================================================================
# Statistics & Reporting
# =============================================================================

def get_layer_stats(loader: DuckDBLoader) -> dict:
    """Get row counts for all tables by layer."""
    return {
        "silver": {
            "events": loader.table_count("silver_events"),
            "items": loader.table_count("silver_items"),
        },
        "gold": {
            "fact_events": loader.table_count("fact_events"),
            "fact_items": loader.table_count("fact_event_items"),
            "dim_items": loader.table_count("dim_items"),
            "dim_users": loader.table_count("dim_users"),
        }
    }


def get_event_summary(loader: DuckDBLoader) -> pd.DataFrame:
    """Get a summary of events by type from Gold layer."""
    return loader.query("""
        SELECT 
            event_name,
            COUNT(*) as event_count,
            COUNT(DISTINCT user_id) as unique_users,
            COUNT(DISTINCT session_id) as sessions,
            MIN(event_timestamp) as first_event,
            MAX(event_timestamp) as last_event
        FROM fact_events
        GROUP BY event_name
        ORDER BY event_count DESC
    """)
