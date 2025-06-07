"""
Snowflake Configuration Module
Week 7-8: Data Warehousing

This module handles Snowflake data warehouse configuration and operations.
"""

import os
import snowflake.connector
from snowflake.connector import DictCursor
from typing import Dict, Any, List, Optional
import logging
import json
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

class SnowflakeConfig:
    """Snowflake configuration and connection management."""
    
    def __init__(self):
        self.account = os.getenv('SNOWFLAKE_ACCOUNT')
        self.user = os.getenv('SNOWFLAKE_USER')
        self.password = os.getenv('SNOWFLAKE_PASSWORD')
        self.warehouse = os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')
        self.database = os.getenv('SNOWFLAKE_DATABASE', 'STREAMIFY_ANALYTICS')
        self.schema = os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')
        self.role = os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN')
        
        self.connection = None
        self.cursor = None
    
    def connect(self) -> bool:
        """Establish connection to Snowflake."""
        try:
            self.connection = snowflake.connector.connect(
                user=self.user,
                password=self.password,
                account=self.account,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema,
                role=self.role
            )
            
            self.cursor = self.connection.cursor(DictCursor)
            logger.info("Connected to Snowflake successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            return False
    
    def disconnect(self):
        """Close Snowflake connection."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("Disconnected from Snowflake")
    
    def test_connection(self) -> bool:
        """Test Snowflake connection and permissions."""
        try:
            if not self.connection:
                if not self.connect():
                    return False
            
            # Test basic query
            self.cursor.execute("SELECT CURRENT_VERSION()")
            result = self.cursor.fetchone()
            logger.info(f"Snowflake version: {result['CURRENT_VERSION()']}")
            
            # Test warehouse access
            self.cursor.execute(f"USE WAREHOUSE {self.warehouse}")
            
            # Test database access
            self.cursor.execute(f"USE DATABASE {self.database}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error testing Snowflake connection: {e}")
            return False
    
    def create_database_and_schema(self) -> bool:
        """Create database and schema if they don't exist."""
        try:
            if not self.connection:
                if not self.connect():
                    return False
            
            # Create database
            self.cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
            logger.info(f"Database '{self.database}' created or already exists")
            
            # Use the database
            self.cursor.execute(f"USE DATABASE {self.database}")
            
            # Create schema
            self.cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
            logger.info(f"Schema '{self.schema}' created or already exists")
            
            # Use the schema
            self.cursor.execute(f"USE SCHEMA {self.schema}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error creating database and schema: {e}")
            return False
    
    def create_tables(self) -> bool:
        """Create data warehouse tables."""
        try:
            if not self.connection:
                if not self.connect():
                    return False
            
            # Raw transactions table
            raw_transactions_sql = """
            CREATE TABLE IF NOT EXISTS raw_transactions (
                transaction_id VARCHAR(255) PRIMARY KEY,
                user_id VARCHAR(255),
                user_email VARCHAR(255),
                user_name VARCHAR(255),
                product_id VARCHAR(255),
                product_name VARCHAR(255),
                product_category VARCHAR(100),
                amount DECIMAL(10,2),
                currency VARCHAR(10),
                payment_method VARCHAR(50),
                status VARCHAR(20),
                transaction_timestamp TIMESTAMP_NTZ,
                location VARIANT,
                behavior_pattern VARCHAR(50),
                session_id VARCHAR(255),
                device_type VARCHAR(50),
                browser VARCHAR(50),
                ip_address VARCHAR(45),
                processing_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
            """
            
            # Fraud alerts table
            fraud_alerts_sql = """
            CREATE TABLE IF NOT EXISTS fraud_alerts (
                alert_id VARCHAR(255) PRIMARY KEY,
                transaction_id VARCHAR(255),
                user_id VARCHAR(255),
                amount DECIMAL(10,2),
                fraud_reason VARCHAR(500),
                fraud_score DECIMAL(3,2),
                alert_level VARCHAR(20),
                alert_timestamp TIMESTAMP_NTZ,
                additional_context VARIANT,
                created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
            """
            
            # Analytics results table
            analytics_results_sql = """
            CREATE TABLE IF NOT EXISTS analytics_results (
                window_id VARCHAR(255) PRIMARY KEY,
                window_start TIMESTAMP_NTZ,
                window_end TIMESTAMP_NTZ,
                window_duration_seconds INTEGER,
                total_transactions INTEGER,
                total_sales DECIMAL(15,2),
                average_transaction_amount DECIMAL(10,2),
                unique_users INTEGER,
                unique_products INTEGER,
                unique_categories INTEGER,
                top_categories VARIANT,
                top_products VARIANT,
                payment_method_distribution VARIANT,
                device_type_distribution VARIANT,
                geographic_distribution VARIANT,
                fraud_rate DECIMAL(5,4),
                conversion_rate DECIMAL(5,4),
                revenue_per_user DECIMAL(10,2),
                transactions_per_user DECIMAL(10,2),
                created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
            """
            
            # KPI metrics table
            kpi_metrics_sql = """
            CREATE TABLE IF NOT EXISTS kpi_metrics (
                metric_id VARCHAR(255) PRIMARY KEY,
                timestamp TIMESTAMP_NTZ,
                total_revenue DECIMAL(15,2),
                total_transactions INTEGER,
                average_order_value DECIMAL(10,2),
                conversion_rate DECIMAL(5,4),
                fraud_rate DECIMAL(5,4),
                top_performing_category VARCHAR(100),
                top_performing_product VARCHAR(255),
                customer_acquisition_rate DECIMAL(10,2),
                revenue_growth_rate DECIMAL(5,2),
                transaction_velocity DECIMAL(10,2),
                created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
            """
            
            # Execute table creation
            tables = [
                ("raw_transactions", raw_transactions_sql),
                ("fraud_alerts", fraud_alerts_sql),
                ("analytics_results", analytics_results_sql),
                ("kpi_metrics", kpi_metrics_sql)
            ]
            
            for table_name, sql in tables:
                self.cursor.execute(sql)
                logger.info(f"Table '{table_name}' created or already exists")
            
            return True
            
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            return False
    
    def create_stages(self) -> bool:
        """Create Snowflake stages for data loading."""
        try:
            if not self.connection:
                if not self.connect():
                    return False
            
            # S3 stage for raw data
            s3_stage_sql = f"""
            CREATE OR REPLACE STAGE s3_raw_data_stage
            URL = 's3://{os.getenv('S3_BUCKET_NAME', 'streamify-analytics-data-lake')}/raw_transactions/'
            CREDENTIALS = (AWS_KEY_ID = '{os.getenv('AWS_ACCESS_KEY_ID')}' 
                          AWS_SECRET_KEY = '{os.getenv('AWS_SECRET_ACCESS_KEY')}')
            FILE_FORMAT = (TYPE = 'PARQUET')
            """
            
            # S3 stage for processed data
            s3_processed_stage_sql = f"""
            CREATE OR REPLACE STAGE s3_processed_data_stage
            URL = 's3://{os.getenv('S3_BUCKET_NAME', 'streamify-analytics-data-lake')}/processed_transactions/'
            CREDENTIALS = (AWS_KEY_ID = '{os.getenv('AWS_ACCESS_KEY_ID')}' 
                          AWS_SECRET_KEY = '{os.getenv('AWS_SECRET_ACCESS_KEY')}')
            FILE_FORMAT = (TYPE = 'PARQUET')
            """
            
            stages = [
                ("s3_raw_data_stage", s3_stage_sql),
                ("s3_processed_data_stage", s3_processed_stage_sql)
            ]
            
            for stage_name, sql in stages:
                self.cursor.execute(sql)
                logger.info(f"Stage '{stage_name}' created or updated")
            
            return True
            
        except Exception as e:
            logger.error(f"Error creating stages: {e}")
            return False
    
    def create_views(self) -> bool:
        """Create analytical views."""
        try:
            if not self.connection:
                if not self.connect():
                    return False
            
            # Daily sales summary view
            daily_sales_view_sql = """
            CREATE OR REPLACE VIEW daily_sales_summary AS
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
            """
            
            # Fraud analysis view
            fraud_analysis_view_sql = """
            CREATE OR REPLACE VIEW fraud_analysis AS
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
            """
            
            # Product performance view
            product_performance_view_sql = """
            CREATE OR REPLACE VIEW product_performance AS
            SELECT 
                product_category,
                product_name,
                COUNT(*) as total_sales,
                SUM(amount) as total_revenue,
                AVG(amount) as avg_price,
                COUNT(DISTINCT user_id) as unique_customers,
                RANK() OVER (PARTITION BY product_category ORDER BY SUM(amount) DESC) as category_rank
            FROM raw_transactions
            WHERE status = 'completed'
            GROUP BY product_category, product_name
            ORDER BY total_revenue DESC
            """
            
            views = [
                ("daily_sales_summary", daily_sales_view_sql),
                ("fraud_analysis", fraud_analysis_view_sql),
                ("product_performance", product_performance_view_sql)
            ]
            
            for view_name, sql in views:
                self.cursor.execute(sql)
                logger.info(f"View '{view_name}' created or updated")
            
            return True
            
        except Exception as e:
            logger.error(f"Error creating views: {e}")
            return False
    
    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute a SQL query and return results."""
        try:
            if not self.connection:
                if not self.connect():
                    return []
            
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            return results
            
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            return []
    
    def get_table_info(self, table_name: str) -> List[Dict[str, Any]]:
        """Get information about a table."""
        query = f"DESCRIBE TABLE {table_name}"
        return self.execute_query(query)
    
    def get_database_summary(self) -> Dict[str, Any]:
        """Get summary of database contents."""
        summary = {}
        
        # Get table counts
        tables = ['raw_transactions', 'fraud_alerts', 'analytics_results', 'kpi_metrics']
        for table in tables:
            count_query = f"SELECT COUNT(*) as count FROM {table}"
            result = self.execute_query(count_query)
            if result:
                summary[f"{table}_count"] = result[0]['COUNT']
        
        # Get database size
        size_query = """
        SELECT 
            SUM(BYTES) as total_bytes,
            COUNT(*) as total_tables
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = 'PUBLIC'
        """
        result = self.execute_query(size_query)
        if result:
            summary['total_bytes'] = result[0]['TOTAL_BYTES']
            summary['total_tables'] = result[0]['TOTAL_TABLES']
        
        return summary
