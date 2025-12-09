"""
Transformer module for the Medallion Architecture Analytics Pipeline.

This module implements the Bronze layer transformation:
- BronzeTransformer: Raw JSON persistence and validation

Architecture Flow:
    SQS Message -> Bronze (Raw JSON) -> Silver (SQL via Loader) -> Gold (SQL via Loader)
"""
import json
import logging
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

from pydantic import ValidationError

from src.models import BronzeMessageModel

logger = logging.getLogger(__name__)


class BronzeTransformer:
    """
    Bronze layer transformer for raw data persistence.
    
    Handles saving raw SQS messages as JSON files partitioned by date.
    No transformation is applied - data is stored exactly as received.
    """

    def __init__(self, bronze_path: str = "data/bronze"):
        """
        Initialize the Bronze transformer.
        
        Args:
            bronze_path: Base path for bronze layer storage.
        """
        self.bronze_path = Path(bronze_path)
        self.bronze_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Bronze transformer initialized with path: {self.bronze_path}")

    def _get_partition_path(self, timestamp: Optional[datetime] = None) -> Path:
        """
        Get the partition path based on timestamp.
        
        Args:
            timestamp: Event timestamp for partitioning. Defaults to now.
            
        Returns:
            Path object for the partition directory.
        """
        ts = timestamp or datetime.utcnow()
        partition = self.bronze_path / ts.strftime("%Y/%m/%d")
        partition.mkdir(parents=True, exist_ok=True)
        return partition

    def _parse_items_firebase_format(self, text: str) -> list[dict]:
        """
        Robust parser for Firebase/Java toString() format.
        
        Args:
            text: String in Firebase/Java toString() format.
            
        Returns:
            List of dictionaries.
        """
        
        def parse_value(val: str):
            val = val.strip()
            if val == 'null' or val == '(not set)':
                return None
            try:
                if '.' in val:
                    return float(val)
                return int(val)
            except ValueError:
                return val
        
        def parse_object(s: str) -> dict:
            result = {}
            depth = 0
            current_key = ""
            current_val = ""
            in_key = True
            
            i = 0
            while i < len(s):
                char = s[i]
                
                if char in '[{':
                    depth += 1
                    if not in_key:
                        current_val += char
                elif char in ']}':
                    depth -= 1
                    if not in_key:
                        current_val += char
                elif char == '=' and depth == 0 and in_key:
                    in_key = False
                elif char == ',' and depth == 0:
                    if current_key:
                        result[current_key.strip()] = parse_nested(current_val.strip())
                    current_key = ""
                    current_val = ""
                    in_key = True
                else:
                    if in_key:
                        current_key += char
                    else:
                        current_val += char
                i += 1
            
            if current_key:
                result[current_key.strip()] = parse_nested(current_val.strip())
            
            return result
        
        def parse_array(s: str) -> list:
            result = []
            depth = 0
            current = ""
            in_object = False
            
            i = 0
            while i < len(s):
                char = s[i]
                
                if char == '[' and not in_object:
                    depth += 1
                elif char == ']' and not in_object:
                    depth -= 1
                elif char == '{':
                    if in_object:
                        current += char
                    in_object = True
                    depth += 1
                elif char == '}':
                    depth -= 1
                    if depth == 1:
                        if current:
                            result.append(parse_object(current))
                            current = ""
                        in_object = False
                    else:
                        current += char
                elif in_object:
                    current += char
                
                i += 1
            
            return result
        
        def parse_nested(s: str):
            s = s.strip()
            if s.startswith('['):
                return parse_array(s)
            elif s.startswith('{'):
                return parse_object(s[1:-1] if s.endswith('}') else s[1:])
            else:
                return parse_value(s)
        
        return parse_nested(text)

    def save_raw_message(
        self,
        message: dict,
        timestamp: Optional[datetime] = None,
    ) -> str:
        """
        Save a raw SQS message to the Bronze layer.
        
        Args:
            message: Raw message dictionary from SQS.
            timestamp: Optional timestamp for partitioning.
            
        Returns:
            Path to the saved JSON file.
        """
        message_id = message.get("message_id", str(uuid.uuid4()))
        partition_path = self._get_partition_path(timestamp)
        
        # Create filename with timestamp for ordering
        file_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
        filename = f"{file_ts}_{message_id}.json"
        file_path = partition_path / filename
        
        items = message["body"]["items"]
        message["body"]["errors_parsing_items"] = False
        if isinstance(items, str):
            try:
                items = self._parse_items_firebase_format(items)
            except Exception as e:
                logger.error(f"Error parsing items: {e}")
                message["body"]["errors_parsing_items"] = True
        message["body"]["items"] = items
        
        try:
            with open(file_path, "w") as f:
                json.dump(message, f, indent=2, default=str)
            logger.debug(f"Saved raw message to Bronze: {file_path}")
            return str(file_path)
        except Exception as e:
            logger.error(f"Error saving raw message to Bronze: {e}")
            return None

    def save_batch(
        self,
        messages: list[dict],
        timestamp: Optional[datetime] = None,
    ) -> list[str]:
        """
        Save a batch of raw messages to the Bronze layer.
        
        Args:
            messages: List of raw message dictionaries.
            timestamp: Optional timestamp for partitioning.
            
        Returns:
            List of paths to saved JSON files.
        """
        saved_paths = []
        for msg in messages:
            try:
                path = self.save_raw_message(msg, timestamp)
                if path:
                    saved_paths.append(path)
            except Exception as e:
                logger.error(f"Error saving message to Bronze: {e}")
        
        logger.info(f"Saved {len(saved_paths)} messages to Bronze layer")
        return saved_paths

    def validate_message(self, message: dict) -> Optional[BronzeMessageModel]:
        """
        Validate a raw message against the Bronze schema.
        
        Args:
            message: Raw message dictionary.
            
        Returns:
            Validated BronzeMessageModel or None if validation fails.
        """
        try:
            return BronzeMessageModel(**message)
        except ValidationError as e:
            logger.warning(f"Bronze validation failed: {e}")
            return None

    def list_bronze_files(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> list[Path]:
        """
        List all Bronze JSON files within a date range.
        
        Args:
            start_date: Start of date range. Defaults to all time.
            end_date: End of date range. Defaults to today.
            
        Returns:
            List of Path objects to Bronze JSON files.
        """
        files = []
        for json_file in self.bronze_path.rglob("*.json"):
            try:
                parts = json_file.relative_to(self.bronze_path).parts
                if len(parts) >= 3:
                    file_date = datetime(int(parts[0]), int(parts[1]), int(parts[2]))
                    if start_date and file_date < start_date:
                        continue
                    if end_date and file_date > end_date:
                        continue
                files.append(json_file)
            except (ValueError, IndexError):
                files.append(json_file)
        
        return sorted(files)
