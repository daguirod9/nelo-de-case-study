"""
Data models and schema definitions for the Medallion Architecture Analytics Pipeline.

This module defines the Pydantic models and DuckDB schemas for:
- Bronze: Raw SQS message validation
- Silver: Parsed and cleaned event data  
- Gold: Dimensional models and fact tables
- Platinum: Aggregated metrics and KPIs

Medallion Architecture:
    Bronze -> Silver -> Gold -> Platinum
    (Raw)    (Clean)   (Modeled)  (Aggregated)
"""
from datetime import datetime
from typing import Optional, Any
from pydantic import BaseModel, Field
import uuid


# =============================================================================
# BRONZE LAYER MODELS - Raw SQS Message Structure
# =============================================================================

class ItemParamValue(BaseModel):
    """Model representing the value structure of an item parameter."""
    string_value: Optional[Any] = None
    int_value: Optional[int] = None
    float_value: Optional[float] = None
    double_value: Optional[float] = None


class ItemParam(BaseModel):
    """Model representing a key-value item parameter."""
    key: str
    value: ItemParamValue


class BronzeItemModel(BaseModel):
    """
    Model representing a raw item from the SQS message.
    
    This model captures all fields from the raw item structure,
    including nested item_params for custom attributes.
    """
    item_id: Optional[str] = None
    item_name: Optional[str] = None
    item_brand: Optional[str] = None
    item_variant: Optional[str] = None
    item_category: Optional[str] = None
    item_category2: Optional[str] = None
    item_category3: Optional[str] = None
    item_category4: Optional[str] = None
    item_category5: Optional[str] = None
    price_in_usd: Optional[float] = None
    price: Optional[float] = None
    quantity: Optional[int] = 1
    item_revenue_in_usd: Optional[float] = None
    item_revenue: Optional[float] = None
    item_refund_in_usd: Optional[float] = None
    item_refund: Optional[float] = None
    coupon: Optional[str] = None
    affiliation: Optional[str] = None
    location_id: Optional[str] = None
    item_list_id: Optional[str] = None
    item_list_name: Optional[str] = None
    item_list_index: Optional[int] = None
    promotion_id: Optional[str] = None
    promotion_name: Optional[str] = None
    creative_name: Optional[str] = None
    creative_slot: Optional[str] = None
    item_params: list[ItemParam] = Field(default_factory=list)


class BronzeEventBody(BaseModel):
    """Model representing the body of a raw SQS event."""
    event_timestamp: int  # Timestamp in microseconds
    user_id: str
    event_name: str  # view_item_list, view_item, begin_checkout, purchase
    platform: str  # IOS or ANDROID
    items: list[dict] = Field(default_factory=list)
    replay_timestamp: Optional[str] = None


class MessageAttribute(BaseModel):
    """Model representing an SQS message attribute."""
    StringValue: Optional[str] = None
    DataType: str = "String"


class BronzeMessageModel(BaseModel):
    """
    Model representing the complete raw SQS message.
    
    This is the entry point for Bronze layer validation.
    The full message structure as received from SQS.
    """
    message_id: str
    receipt_handle: str
    body: BronzeEventBody
    attributes: dict = Field(default_factory=dict)
    message_attributes: dict[str, MessageAttribute] = Field(default_factory=dict)

# =============================================================================
# DUCKDB SCHEMA DEFINITIONS
# =============================================================================

SILVER_EVENTS_SCHEMA = """
CREATE TABLE IF NOT EXISTS silver_events (
    event_id VARCHAR PRIMARY KEY,
    message_id VARCHAR NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    event_timestamp_micros BIGINT NOT NULL,
    user_id VARCHAR NOT NULL,
    event_name VARCHAR NOT NULL,
    platform VARCHAR NOT NULL,
    replay_timestamp TIMESTAMP,
    received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_silver_events_timestamp ON silver_events(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_silver_events_user ON silver_events(user_id);
CREATE INDEX IF NOT EXISTS idx_silver_events_name ON silver_events(event_name);
CREATE INDEX IF NOT EXISTS idx_silver_events_message ON silver_events(message_id);
"""

SILVER_ITEMS_SCHEMA = """
CREATE TABLE IF NOT EXISTS silver_items (
    item_record_id VARCHAR PRIMARY KEY,
    event_id VARCHAR NOT NULL,
    item_id VARCHAR,
    item_name VARCHAR,
    item_brand VARCHAR,
    item_variant VARCHAR,
    item_category VARCHAR,
    item_category2 VARCHAR,
    item_category3 VARCHAR,
    item_category4 VARCHAR,
    item_category5 VARCHAR,
    price_in_usd DECIMAL(12, 4),
    price DECIMAL(12, 4),
    quantity INTEGER DEFAULT 1,
    item_revenue_in_usd DECIMAL(12, 4),
    item_revenue DECIMAL(12, 4),
    item_refund_in_usd DECIMAL(12, 4),
    item_refund DECIMAL(12, 4),
    coupon VARCHAR,
    affiliation VARCHAR,
    location_id VARCHAR,
    item_list_id VARCHAR,
    item_list_name VARCHAR,
    item_list_index INTEGER,
    promotion_id VARCHAR,
    promotion_name VARCHAR,
    creative_name VARCHAR,
    creative_slot VARCHAR,
    in_stock INTEGER,
    discounts VARCHAR,
    discount_amount DECIMAL(12, 4),
    total_price DECIMAL(12, 4),
    number_of_installments INTEGER,
    installment_price VARCHAR
);

CREATE INDEX IF NOT EXISTS idx_silver_items_event ON silver_items(event_id);
CREATE INDEX IF NOT EXISTS idx_silver_items_item ON silver_items(item_id);
CREATE INDEX IF NOT EXISTS idx_silver_items_list ON silver_items(item_list_name);
"""

GOLD_FACT_EVENTS_SCHEMA = """
CREATE TABLE IF NOT EXISTS fact_events (
    event_id VARCHAR PRIMARY KEY,
    event_timestamp TIMESTAMP NOT NULL,
    user_id VARCHAR NOT NULL,
    event_name VARCHAR NOT NULL,
    platform VARCHAR NOT NULL,
    session_id VARCHAR,
    event_date VARCHAR NOT NULL,
    event_hour INTEGER NOT NULL,
    raw_message_id VARCHAR,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fact_events_timestamp ON fact_events(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_fact_events_user ON fact_events(user_id);
CREATE INDEX IF NOT EXISTS idx_fact_events_name ON fact_events(event_name);
CREATE INDEX IF NOT EXISTS idx_fact_events_date ON fact_events(event_date);
CREATE INDEX IF NOT EXISTS idx_fact_events_session ON fact_events(session_id);
"""

GOLD_FACT_EVENT_ITEMS_SCHEMA = """
CREATE TABLE IF NOT EXISTS fact_event_items (
    event_item_id VARCHAR PRIMARY KEY,
    event_id VARCHAR NOT NULL,
    item_id VARCHAR,
    item_name VARCHAR,
    item_list_name VARCHAR,
    item_list_id VARCHAR,
    item_category VARCHAR,
    item_brand VARCHAR,
    price DECIMAL(12, 4),
    total_price DECIMAL(12, 4),
    quantity INTEGER DEFAULT 1,
    position_in_list INTEGER,
    has_discount BOOLEAN DEFAULT FALSE,
    discount_amount DECIMAL(12, 4),
    in_stock BOOLEAN,
    FOREIGN KEY (event_id) REFERENCES fact_events(event_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_items_event ON fact_event_items(event_id);
CREATE INDEX IF NOT EXISTS idx_fact_items_item ON fact_event_items(item_id);
CREATE INDEX IF NOT EXISTS idx_fact_items_list ON fact_event_items(item_list_name);
"""

GOLD_DIM_ITEMS_SCHEMA = """
CREATE TABLE IF NOT EXISTS dim_items (
    item_sk VARCHAR PRIMARY KEY,
    item_id VARCHAR NOT NULL,
    item_name VARCHAR,
    item_brand VARCHAR,
    item_category VARCHAR,
    item_category2 VARCHAR,
    item_category3 VARCHAR,
    item_category4 VARCHAR,
    item_category5 VARCHAR,
    first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_dim_items_id ON dim_items(item_id);
CREATE INDEX IF NOT EXISTS idx_dim_items_current ON dim_items(is_current);
"""

GOLD_DIM_USERS_SCHEMA = """
CREATE TABLE IF NOT EXISTS dim_users (
    user_sk VARCHAR PRIMARY KEY,
    user_id VARCHAR NOT NULL,
    first_platform VARCHAR,
    last_platform VARCHAR,
    first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_sessions INTEGER DEFAULT 0,
    is_current BOOLEAN DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_dim_users_id ON dim_users(user_id);
CREATE INDEX IF NOT EXISTS idx_dim_users_current ON dim_users(is_current);
"""


# Schema collections by layer
SILVER_SCHEMAS = [SILVER_EVENTS_SCHEMA, SILVER_ITEMS_SCHEMA]
GOLD_SCHEMAS = [
    GOLD_FACT_EVENTS_SCHEMA,
    GOLD_FACT_EVENT_ITEMS_SCHEMA,
    GOLD_DIM_ITEMS_SCHEMA,
    GOLD_DIM_USERS_SCHEMA,
]

