#!/usr/bin/env python3
"""
Main orchestrator for the Item Analytics Pipeline.

Data flow through Medallion Architecture:
1. Bronze: Raw SQS message persistence (JSON files)
2. Silver: Parse Bronze JSONs via DuckDB SQL models
3. Gold: Transform to dimensional models via DuckDB SQL models

Usage:
    python deliverable2_pipeline.py --layer silver     # Process only through Silver layer
    python deliverable2_pipeline.py --layer gold       # Process through Silver + Gold layers
"""
import argparse
import logging
import signal
import sys
import time
from pathlib import Path

import config
from src.consumer import SQSConsumer
from src.transformer import BronzeTransformer
from src.loader import (
    DuckDBLoader,
    initialize_schemas,
    transform_to_silver,
    transform_to_gold,
    export_silver_to_parquet,
    export_gold_to_parquet,
    get_layer_stats,
)

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(config.LOG_FILE),
    ],
)
logger = logging.getLogger(__name__)

running = True


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    global running
    logger.info("Shutdown signal received, stopping gracefully...")
    running = False


def process_batch(
    consumer: SQSConsumer,
    bronze: BronzeTransformer,
    loader: DuckDBLoader,
    target_layer: str = "gold",
    delete_after_processing: bool = False,
) -> dict:
    """
    Process a batch of messages through the Medallion architecture.
    
    Args:
        consumer: SQS consumer instance.
        bronze: Bronze transformer instance.
        loader: DuckDBLoader instance.
        target_layer: Target layer ('bronze', 'silver', 'gold').
        delete_after_processing: Whether to delete messages after processing.
        
    Returns:
        Dictionary with processing statistics.
    """
    stats = {
        "messages_received": 0,
        "bronze_saved": 0,
        "silver_events": 0,
        "silver_items": 0,
        "gold_events": 0,
        "gold_items": 0,
        "errors": 0,
    }
    
    messages = consumer.receive_messages()
    stats["messages_received"] = len(messages)
    
    if not messages:
        return stats
    
    logger.info(f"Received {len(messages)} messages from SQS")
    
    try:
        # === BRONZE ===
        bronze_paths = bronze.save_batch(messages)
        stats["bronze_saved"] = len(bronze_paths)
        logger.info(f"Bronze: Saved {len(bronze_paths)} raw messages")
        
        if target_layer == "bronze":
            return stats
        
        # === SILVER ===
        silver_result = transform_to_silver(loader)
        stats["silver_events"] = silver_result["events_inserted"]
        stats["silver_items"] = silver_result["items_inserted"]
        
        if config.SAVE_PARQUET:
            export_silver_to_parquet(loader)
        
        if target_layer == "silver":
            return stats
        
        # === GOLD ===
        gold_result = transform_to_gold(loader)
        stats["gold_events"] = gold_result["fact_events_inserted"]
        stats["gold_items"] = gold_result["fact_items_inserted"]
        
        if config.SAVE_PARQUET:
            export_gold_to_parquet(loader)
        
        # Delete processed messages
        if delete_after_processing:
            for msg in messages:
                consumer.delete_message(msg["receipt_handle"])
                    
    except Exception as e:
        logger.error(f"Error processing batch: {e}")
        stats["errors"] += 1
        raise
    
    return stats


def run_pipeline(
    consumer: SQSConsumer,
    bronze: BronzeTransformer,
    loader: DuckDBLoader,
    target_layer: str = "gold",
):
    """
    Run continuous polling loop through the Medallion architecture.
    """
    global running
    
    logger.info(f"Starting pipeline - Target: {target_layer}, Interval: {config.POLLING_INTERVAL}s")
    logger.info("Press Ctrl+C to stop")
    
    totals = {"messages": 0, "bronze": 0, "silver": 0, "gold": 0, "batches": 0}
    
    while running:
        try:
            stats = process_batch(consumer, bronze, loader, target_layer)
            
            if stats["messages_received"] > 0:
                totals["messages"] += stats["messages_received"]
                totals["bronze"] += stats["bronze_saved"]
                totals["silver"] += stats["silver_events"]
                totals["gold"] += stats["gold_events"]
                totals["batches"] += 1
                
                logger.info(
                    f"Batch #{totals['batches']}: "
                    f"Bronze={stats['bronze_saved']}, "
                    f"Silver={stats['silver_events']}, "
                    f"Gold={stats['gold_events']}"
                )
            
            if running:
                time.sleep(config.POLLING_INTERVAL)
                
        except KeyboardInterrupt:
            running = False
        except Exception as e:
            logger.error(f"Error in pipeline: {e}")
            if running:
                time.sleep(config.POLLING_INTERVAL)
    
    logger.info(f"Pipeline stopped. Totals: {totals}")


def main():
    parser = argparse.ArgumentParser(description="Item Analytics Pipeline")
    parser.add_argument(
        "--layer", 
        type=str, 
        default="gold", 
        choices=["bronze", "silver", "gold"],
        help="Target layer to process through"
    )
    args = parser.parse_args()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    config.ensure_directories()
    
    logger.info(f"Initializing pipeline → {args.layer} layer")
    
    try:
        loader = DuckDBLoader(data_path=config.DATA_PATH)
        initialize_schemas(loader)
        
        views_path = Path("sql/create_views.sql")
        if views_path.exists():
            loader.execute_file(str(views_path))
        
        bronze = BronzeTransformer(config.BRONZE_PATH)
        consumer = SQSConsumer()
        
        try:
            logger.info(f"Queue messages: ~{consumer.get_approximate_message_count()}")
        except:
            pass
        
        run_pipeline(consumer, bronze, loader, target_layer=args.layer)
        
        logger.info(f"Final stats: {get_layer_stats(loader)}")
        loader.close()
        
    except Exception as e:
        logger.error(f"Pipeline error: {e}")
        raise


if __name__ == "__main__":
    main()
