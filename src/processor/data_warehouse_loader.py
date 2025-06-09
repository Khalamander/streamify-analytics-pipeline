"""
Data Warehouse Loader
Week 8: Data Warehousing Pipeline Components

This module handles loading data from S3 data lake to Snowflake data warehouse.
"""

import json
import logging
import pandas as pd
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
import os
from dotenv import load_dotenv

from src.config.aws_config import AWSConfig, S3DataLakeManager
from src.config.snowflake_config import SnowflakeConfig

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

class DataWarehouseLoader:
    """Handles data loading from S3 to Snowflake."""
    
    def __init__(self):
        self.aws_config = AWSConfig()
        self.s3_manager = S3DataLakeManager(self.aws_config)
        self.snowflake_config = SnowflakeConfig()
        
        # Initialize connections
        self._initialize_connections()
    
    def _initialize_connections(self):
        """Initialize AWS and Snowflake connections."""
        try:
            # Test AWS connection
            if not self.aws_config.test_connection():
                raise Exception("AWS connection failed")
            
            # Test Snowflake connection
            if not self.snowflake_config.test_connection():
                raise Exception("Snowflake connection failed")
            
            logger.info("All connections initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize connections: {e}")
            raise
    
    def load_raw_transactions(self, s3_prefix: str, batch_size: int = 1000) -> bool:
        """Load raw transaction data from S3 to Snowflake."""
        try:
            logger.info(f"Loading raw transactions from S3 prefix: {s3_prefix}")
            
            # Get list of Parquet files from S3
            s3_objects = self.aws_config.list_s3_objects(s3_prefix)
            parquet_files = [obj for obj in s3_objects if obj['Key'].endswith('.parquet')]
            
            if not parquet_files:
                logger.warning("No Parquet files found in S3 prefix")
                return False
            
            logger.info(f"Found {len(parquet_files)} Parquet files to process")
            
            # Process files in batches
            for i in range(0, len(parquet_files), batch_size):
                batch_files = parquet_files[i:i + batch_size]
                self._process_batch_files(batch_files, 'raw_transactions')
            
            logger.info("Raw transactions loaded successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error loading raw transactions: {e}")
            return False
    
    def load_fraud_alerts(self, s3_prefix: str, batch_size: int = 1000) -> bool:
        """Load fraud alert data from S3 to Snowflake."""
        try:
            logger.info(f"Loading fraud alerts from S3 prefix: {s3_prefix}")
            
            # Get list of JSON files from S3
            s3_objects = self.aws_config.list_s3_objects(s3_prefix)
            json_files = [obj for obj in s3_objects if obj['Key'].endswith('.json')]
            
            if not json_files:
                logger.warning("No JSON files found in S3 prefix")
                return False
            
            logger.info(f"Found {len(json_files)} JSON files to process")
            
            # Process files in batches
            for i in range(0, len(json_files), batch_size):
                batch_files = json_files[i:i + batch_size]
                self._process_batch_files(batch_files, 'fraud_alerts')
            
            logger.info("Fraud alerts loaded successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error loading fraud alerts: {e}")
            return False
    
    def load_analytics_results(self, s3_prefix: str, batch_size: int = 1000) -> bool:
        """Load analytics results from S3 to Snowflake."""
        try:
            logger.info(f"Loading analytics results from S3 prefix: {s3_prefix}")
            
            # Get list of JSON files from S3
            s3_objects = self.aws_config.list_s3_objects(s3_prefix)
            json_files = [obj for obj in s3_objects if obj['Key'].endswith('.json')]
            
            if not json_files:
                logger.warning("No JSON files found in S3 prefix")
                return False
            
            logger.info(f"Found {len(json_files)} JSON files to process")
            
            # Process files in batches
            for i in range(0, len(json_files), batch_size):
                batch_files = json_files[i:i + batch_size]
                self._process_batch_files(batch_files, 'analytics_results')
            
            logger.info("Analytics results loaded successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error loading analytics results: {e}")
            return False
    
    def _process_batch_files(self, files: List[Dict[str, Any]], table_name: str):
        """Process a batch of files and load to Snowflake."""
        try:
            # Download files locally
            local_files = []
            for file_info in files:
                local_path = f"/tmp/{os.path.basename(file_info['Key'])}"
                if self.aws_config.download_from_s3(file_info['Key'], local_path):
                    local_files.append(local_path)
            
            if not local_files:
                logger.warning("No files downloaded successfully")
                return
            
            # Load data to Snowflake
            if table_name == 'raw_transactions':
                self._load_parquet_to_snowflake(local_files, table_name)
            else:
                self._load_json_to_snowflake(local_files, table_name)
            
            # Clean up local files
            for local_file in local_files:
                try:
                    os.remove(local_file)
                except:
                    pass
            
        except Exception as e:
            logger.error(f"Error processing batch files: {e}")
    
    def _load_parquet_to_snowflake(self, file_paths: List[str], table_name: str):
        """Load Parquet files to Snowflake."""
        try:
            # Read Parquet files
            dfs = []
            for file_path in file_paths:
                df = pd.read_parquet(file_path)
                dfs.append(df)
            
            if not dfs:
                return
            
            # Combine dataframes
            combined_df = pd.concat(dfs, ignore_index=True)
            
            # Prepare data for Snowflake
            self._insert_dataframe_to_snowflake(combined_df, table_name)
            
        except Exception as e:
            logger.error(f"Error loading Parquet to Snowflake: {e}")
    
    def _load_json_to_snowflake(self, file_paths: List[str], table_name: str):
        """Load JSON files to Snowflake."""
        try:
            all_data = []
            
            for file_path in file_paths:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        all_data.extend(data)
                    else:
                        all_data.append(data)
            
            if not all_data:
                return
            
            # Convert to DataFrame
            df = pd.DataFrame(all_data)
            
            # Prepare data for Snowflake
            self._insert_dataframe_to_snowflake(df, table_name)
            
        except Exception as e:
            logger.error(f"Error loading JSON to Snowflake: {e}")
    
    def _insert_dataframe_to_snowflake(self, df: pd.DataFrame, table_name: str):
        """Insert DataFrame data into Snowflake table."""
        try:
            if not self.snowflake_config.connection:
                if not self.snowflake_config.connect():
                    return
            
            # Prepare data for insertion
            records = df.to_dict('records')
            
            # Generate insert statements based on table
            if table_name == 'raw_transactions':
                self._insert_raw_transactions(records)
            elif table_name == 'fraud_alerts':
                self._insert_fraud_alerts(records)
            elif table_name == 'analytics_results':
                self._insert_analytics_results(records)
            
        except Exception as e:
            logger.error(f"Error inserting DataFrame to Snowflake: {e}")
    
    def _insert_raw_transactions(self, records: List[Dict[str, Any]]):
        """Insert raw transaction records."""
        insert_sql = """
        INSERT INTO raw_transactions (
            transaction_id, user_id, user_email, user_name, product_id, product_name,
            product_category, amount, currency, payment_method, status, transaction_timestamp,
            location, behavior_pattern, session_id, device_type, browser, ip_address
        ) VALUES (
            %(transaction_id)s, %(user_id)s, %(user_email)s, %(user_name)s, %(product_id)s,
            %(product_name)s, %(product_category)s, %(amount)s, %(currency)s, %(payment_method)s,
            %(status)s, %(transaction_timestamp)s, %(location)s, %(behavior_pattern)s,
            %(session_id)s, %(device_type)s, %(browser)s, %(ip_address)s
        )
        """
        
        self.snowflake_config.cursor.executemany(insert_sql, records)
        self.snowflake_config.connection.commit()
        logger.info(f"Inserted {len(records)} raw transaction records")
    
    def _insert_fraud_alerts(self, records: List[Dict[str, Any]]):
        """Insert fraud alert records."""
        insert_sql = """
        INSERT INTO fraud_alerts (
            alert_id, transaction_id, user_id, amount, fraud_reason, fraud_score,
            alert_level, alert_timestamp, additional_context
        ) VALUES (
            %(alert_id)s, %(transaction_id)s, %(user_id)s, %(amount)s, %(fraud_reason)s,
            %(fraud_score)s, %(alert_level)s, %(alert_timestamp)s, %(additional_context)s
        )
        """
        
        # Generate alert_id if not present
        for record in records:
            if 'alert_id' not in record:
                record['alert_id'] = f"alert_{record.get('transaction_id', 'unknown')}_{int(datetime.now().timestamp())}"
        
        self.snowflake_config.cursor.executemany(insert_sql, records)
        self.snowflake_config.connection.commit()
        logger.info(f"Inserted {len(records)} fraud alert records")
    
    def _insert_analytics_results(self, records: List[Dict[str, Any]]):
        """Insert analytics result records."""
        insert_sql = """
        INSERT INTO analytics_results (
            window_id, window_start, window_end, window_duration_seconds, total_transactions,
            total_sales, average_transaction_amount, unique_users, unique_products, unique_categories,
            top_categories, top_products, payment_method_distribution, device_type_distribution,
            geographic_distribution, fraud_rate, conversion_rate, revenue_per_user, transactions_per_user
        ) VALUES (
            %(window_id)s, %(window_start)s, %(window_end)s, %(window_duration_seconds)s,
            %(total_transactions)s, %(total_sales)s, %(average_transaction_amount)s, %(unique_users)s,
            %(unique_products)s, %(unique_categories)s, %(top_categories)s, %(top_products)s,
            %(payment_method_distribution)s, %(device_type_distribution)s, %(geographic_distribution)s,
            %(fraud_rate)s, %(conversion_rate)s, %(revenue_per_user)s, %(transactions_per_user)s
        )
        """
        
        # Generate window_id if not present
        for record in records:
            if 'window_id' not in record:
                record['window_id'] = f"window_{record.get('window_start', 'unknown')}_{record.get('window_end', 'unknown')}"
        
        self.snowflake_config.cursor.executemany(insert_sql, records)
        self.snowflake_config.connection.commit()
        logger.info(f"Inserted {len(records)} analytics result records")
    
    def run_full_data_load(self) -> bool:
        """Run a full data load from S3 to Snowflake."""
        try:
            logger.info("Starting full data load from S3 to Snowflake")
            
            # Get current timestamp for S3 prefix
            current_time = datetime.now(timezone.utc)
            date_prefix = current_time.strftime('%Y/%m/%d')
            
            # Load different data types
            success = True
            
            # Load raw transactions
            raw_prefix = f"raw_transactions/year={current_time.year}/month={current_time.month:02d}/day={current_time.day:02d}/"
            if not self.load_raw_transactions(raw_prefix):
                logger.warning("Failed to load some raw transactions")
                success = False
            
            # Load fraud alerts
            fraud_prefix = f"fraud_alerts/year={current_time.year}/month={current_time.month:02d}/day={current_time.day:02d}/"
            if not self.load_fraud_alerts(fraud_prefix):
                logger.warning("Failed to load some fraud alerts")
                success = False
            
            # Load analytics results
            analytics_prefix = f"analytics_results/year={current_time.year}/month={current_time.month:02d}/day={current_time.day:02d}/"
            if not self.load_analytics_results(analytics_prefix):
                logger.warning("Failed to load some analytics results")
                success = False
            
            if success:
                logger.info("Full data load completed successfully")
            else:
                logger.warning("Full data load completed with some warnings")
            
            return success
            
        except Exception as e:
            logger.error(f"Error in full data load: {e}")
            return False
    
    def get_loading_statistics(self) -> Dict[str, Any]:
        """Get statistics about data loading."""
        try:
            if not self.snowflake_config.connection:
                if not self.snowflake_config.connect():
                    return {}
            
            # Get table counts
            stats = self.snowflake_config.get_database_summary()
            
            # Get recent loading activity
            recent_activity_query = """
            SELECT 
                'raw_transactions' as table_name,
                COUNT(*) as record_count,
                MAX(created_at) as last_updated
            FROM raw_transactions
            WHERE created_at >= CURRENT_TIMESTAMP() - INTERVAL '24 hours'
            UNION ALL
            SELECT 
                'fraud_alerts' as table_name,
                COUNT(*) as record_count,
                MAX(created_at) as last_updated
            FROM fraud_alerts
            WHERE created_at >= CURRENT_TIMESTAMP() - INTERVAL '24 hours'
            UNION ALL
            SELECT 
                'analytics_results' as table_name,
                COUNT(*) as record_count,
                MAX(created_at) as last_updated
            FROM analytics_results
            WHERE created_at >= CURRENT_TIMESTAMP() - INTERVAL '24 hours'
            """
            
            recent_activity = self.snowflake_config.execute_query(recent_activity_query)
            stats['recent_activity'] = recent_activity
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting loading statistics: {e}")
            return {}
    
    def cleanup(self):
        """Clean up connections."""
        if self.snowflake_config:
            self.snowflake_config.disconnect()
        logger.info("Data warehouse loader cleanup completed")
