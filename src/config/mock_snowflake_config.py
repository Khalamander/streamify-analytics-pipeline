"""
Mock Snowflake Configuration for Testing
This simulates Snowflake without actual cloud costs
"""

import json
import sqlite3
import os
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

class MockSnowflakeConfig:
    """Mock Snowflake configuration for local testing without cloud costs."""
    
    def __init__(self):
        self.account = 'mock_account'
        self.user = 'mock_user'
        self.password = 'mock_password'
        self.warehouse = 'COMPUTE_WH'
        self.database = 'STREAMIFY_ANALYTICS'
        self.schema = 'PUBLIC'
        self.role = 'ACCOUNTADMIN'
        
        # Create local SQLite database for mocking
        self.db_path = 'mock_snowflake.db'
        self.connection = None
        self.cursor = None
        
        self._initialize_mock_database()
    
    def _initialize_mock_database(self):
        """Initialize mock SQLite database."""
        try:
            self.connection = sqlite3.connect(self.db_path)
            self.cursor = self.connection.cursor()
            
            # Create tables
            self._create_mock_tables()
            
            logger.info(f"Mock Snowflake database initialized: {self.db_path}")
            
        except Exception as e:
            logger.error(f"Failed to initialize mock database: {e}")
            raise
    
    def _create_mock_tables(self):
        """Create mock tables."""
        tables = {
            'raw_transactions': '''
                CREATE TABLE IF NOT EXISTS raw_transactions (
                    transaction_id TEXT PRIMARY KEY,
                    user_id TEXT,
                    user_email TEXT,
                    user_name TEXT,
                    product_id TEXT,
                    product_name TEXT,
                    product_category TEXT,
                    amount REAL,
                    currency TEXT,
                    payment_method TEXT,
                    status TEXT,
                    transaction_timestamp TEXT,
                    location TEXT,
                    behavior_pattern TEXT,
                    session_id TEXT,
                    device_type TEXT,
                    browser TEXT,
                    ip_address TEXT,
                    processing_timestamp TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''',
            'fraud_alerts': '''
                CREATE TABLE IF NOT EXISTS fraud_alerts (
                    alert_id TEXT PRIMARY KEY,
                    transaction_id TEXT,
                    user_id TEXT,
                    amount REAL,
                    fraud_reason TEXT,
                    fraud_score REAL,
                    alert_level TEXT,
                    alert_timestamp TEXT,
                    additional_context TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''',
            'analytics_results': '''
                CREATE TABLE IF NOT EXISTS analytics_results (
                    window_id TEXT PRIMARY KEY,
                    window_start TEXT,
                    window_end TEXT,
                    window_duration_seconds INTEGER,
                    total_transactions INTEGER,
                    total_sales REAL,
                    average_transaction_amount REAL,
                    unique_users INTEGER,
                    unique_products INTEGER,
                    unique_categories INTEGER,
                    top_categories TEXT,
                    top_products TEXT,
                    payment_method_distribution TEXT,
                    device_type_distribution TEXT,
                    geographic_distribution TEXT,
                    fraud_rate REAL,
                    conversion_rate REAL,
                    revenue_per_user REAL,
                    transactions_per_user REAL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''',
            'kpi_metrics': '''
                CREATE TABLE IF NOT EXISTS kpi_metrics (
                    metric_id TEXT PRIMARY KEY,
                    timestamp TEXT,
                    total_revenue REAL,
                    total_transactions INTEGER,
                    average_order_value REAL,
                    conversion_rate REAL,
                    fraud_rate REAL,
                    top_performing_category TEXT,
                    top_performing_product TEXT,
                    customer_acquisition_rate REAL,
                    revenue_growth_rate REAL,
                    transaction_velocity REAL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            '''
        }
        
        for table_name, sql in tables.items():
            self.cursor.execute(sql)
            logger.info(f"Mock table '{table_name}' created")
        
        self.connection.commit()
    
    def connect(self) -> bool:
        """Mock connection - always succeeds."""
        logger.info("Mock Snowflake connection established")
        return True
    
    def disconnect(self):
        """Mock disconnection."""
        if self.connection:
            self.connection.close()
        logger.info("Mock Snowflake disconnected")
    
    def test_connection(self) -> bool:
        """Mock connection test."""
        try:
            self.cursor.execute("SELECT 1")
            result = self.cursor.fetchone()
            logger.info("Mock Snowflake connection test passed")
            return True
        except Exception as e:
            logger.error(f"Mock connection test failed: {e}")
            return False
    
    def create_database_and_schema(self) -> bool:
        """Mock database creation."""
        logger.info("Mock database and schema created")
        return True
    
    def create_tables(self) -> bool:
        """Mock table creation."""
        logger.info("Mock tables created")
        return True
    
    def create_views(self) -> bool:
        """Mock view creation."""
        views = {
            'daily_sales_summary': '''
                CREATE VIEW IF NOT EXISTS daily_sales_summary AS
                SELECT 
                    DATE(transaction_timestamp) as sale_date,
                    COUNT(*) as total_transactions,
                    SUM(amount) as total_revenue,
                    AVG(amount) as avg_transaction_value,
                    COUNT(DISTINCT user_id) as unique_customers,
                    COUNT(DISTINCT product_category) as categories_sold,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_transactions,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_transactions
                FROM raw_transactions
                GROUP BY DATE(transaction_timestamp)
                ORDER BY sale_date DESC
            ''',
            'fraud_analysis': '''
                CREATE VIEW IF NOT EXISTS fraud_analysis AS
                SELECT 
                    DATE(alert_timestamp) as alert_date,
                    COUNT(*) as total_alerts,
                    COUNT(DISTINCT user_id) as affected_users,
                    AVG(fraud_score) as avg_fraud_score,
                    COUNT(CASE WHEN alert_level = 'CRITICAL' THEN 1 END) as critical_alerts,
                    COUNT(CASE WHEN alert_level = 'HIGH' THEN 1 END) as high_alerts,
                    COUNT(CASE WHEN alert_level = 'MEDIUM' THEN 1 END) as medium_alerts,
                    COUNT(CASE WHEN alert_level = 'LOW' THEN 1 END) as low_alerts
                FROM fraud_alerts
                GROUP BY DATE(alert_timestamp)
                ORDER BY alert_date DESC
            ''',
            'product_performance': '''
                CREATE VIEW IF NOT EXISTS product_performance AS
                SELECT 
                    product_category,
                    product_name,
                    COUNT(*) as total_sales,
                    SUM(amount) as total_revenue,
                    AVG(amount) as avg_price,
                    COUNT(DISTINCT user_id) as unique_customers
                FROM raw_transactions
                WHERE status = 'completed'
                GROUP BY product_category, product_name
                ORDER BY total_revenue DESC
            '''
        }
        
        for view_name, sql in views.items():
            self.cursor.execute(sql)
            logger.info(f"Mock view '{view_name}' created")
        
        self.connection.commit()
        return True
    
    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute mock query and return results."""
        try:
            self.cursor.execute(query)
            columns = [description[0] for description in self.cursor.description]
            rows = self.cursor.fetchall()
            
            results = []
            for row in rows:
                results.append(dict(zip(columns, row)))
            
            return results
            
        except Exception as e:
            logger.error(f"Mock query execution failed: {e}")
            return []
    
    def get_table_info(self, table_name: str) -> List[Dict[str, Any]]:
        """Get mock table information."""
        query = f"PRAGMA table_info({table_name})"
        return self.execute_query(query)
    
    def get_database_summary(self) -> Dict[str, Any]:
        """Get mock database summary."""
        summary = {}
        
        # Get table counts
        tables = ['raw_transactions', 'fraud_alerts', 'analytics_results', 'kpi_metrics']
        for table in tables:
            count_query = f"SELECT COUNT(*) as count FROM {table}"
            result = self.execute_query(count_query)
            if result:
                summary[f"{table}_count"] = result[0]['count']
        
        # Get database size
        summary['total_bytes'] = os.path.getsize(self.db_path) if os.path.exists(self.db_path) else 0
        summary['total_tables'] = len(tables)
        
        return summary
