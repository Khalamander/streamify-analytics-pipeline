#!/usr/bin/env python3
"""
Streamify Analytics Pipeline - Stream Processor
Week 3-5: Stream Processing with PySpark

This module processes real-time transaction data from Kafka using PySpark Streaming
for fraud detection and live analytics.
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Dict, Any, List
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, sum as spark_sum, count, avg, max as spark_max,
    min as spark_min, collect_list, expr, current_timestamp, lit, when,
    udf, struct, to_json
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
    TimestampType, MapType, LongType, BooleanType
)
from pyspark.sql.streaming import StreamingQuery

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class StreamifyStreamProcessor:
    """Main stream processing class for fraud detection and analytics."""
    
    def __init__(self):
        self.spark = None
        self.setup_spark_session()
        self.setup_schemas()
        
        # Fraud detection thresholds
        self.fraud_thresholds = {
            'high_amount': float(os.getenv('FRAUD_HIGH_AMOUNT_THRESHOLD', '2000.0')),
            'rapid_transactions': int(os.getenv('FRAUD_RAPID_TRANSACTIONS_COUNT', '3')),
            'rapid_transactions_window': int(os.getenv('FRAUD_RAPID_TRANSACTIONS_WINDOW', '5'))  # seconds
        }
    
    def setup_spark_session(self):
        """Initialize Spark session with Kafka integration."""
        try:
            self.spark = SparkSession.builder \
                .appName("StreamifyAnalyticsProcessor") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.streaming.checkpointLocation", "./checkpoints") \
                .getOrCreate()
            
            # Set log level to reduce noise
            self.spark.sparkContext.setLogLevel("WARN")
            
            logger.info("Spark session initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Spark session: {e}")
            raise
    
    def setup_schemas(self):
        """Define schemas for transaction data."""
        # Location schema
        self.location_schema = StructType([
            StructField("country", StringType(), True),
            StructField("state", StringType(), True),
            StructField("city", StringType(), True),
            StructField("zip_code", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True)
        ])
        
        # Main transaction schema
        self.transaction_schema = StructType([
            StructField("transaction_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("user_email", StringType(), True),
            StructField("user_name", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("product_category", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("payment_method", StringType(), True),
            StructField("status", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("location", self.location_schema, True),
            StructField("behavior_pattern", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField("device_type", StringType(), True),
            StructField("browser", StringType(), True),
            StructField("ip_address", StringType(), True)
        ])
    
    def create_kafka_stream(self, topic: str) -> Any:
        """Create a Kafka stream for the given topic."""
        kafka_config = {
            "kafka.bootstrap.servers": os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
            "subscribe": topic,
            "startingOffsets": "latest",
            "failOnDataLoss": "false",
            "kafka.security.protocol": "PLAINTEXT"
        }
        
        return self.spark \
            .readStream \
            .format("kafka") \
            .options(**kafka_config) \
            .load()
    
    def parse_transaction_data(self, kafka_df: Any) -> Any:
        """Parse JSON transaction data from Kafka."""
        return kafka_df.select(
            col("key").cast("string").alias("kafka_key"),
            col("timestamp").alias("kafka_timestamp"),
            from_json(col("value").cast("string"), self.transaction_schema).alias("data")
        ).select(
            col("kafka_key"),
            col("kafka_timestamp"),
            col("data.*")
        ).withColumn(
            "processing_timestamp", current_timestamp()
        )
    
    def detect_fraud(self, transactions_df: Any) -> Any:
        """Detect potentially fraudulent transactions."""
        
        # High amount fraud detection
        high_amount_fraud = transactions_df.filter(
            col("amount") > self.fraud_thresholds['high_amount']
        ).withColumn(
            "fraud_reason", lit("High amount transaction")
        ).withColumn(
            "fraud_score", 
            when(col("amount") > self.fraud_thresholds['high_amount'] * 2, 0.9)
            .otherwise(0.7)
        )
        
        # Rapid transactions fraud detection (using window functions)
        rapid_transactions_fraud = transactions_df \
            .withWatermark("processing_timestamp", "10 seconds") \
            .groupBy(
                col("user_id"),
                window(col("processing_timestamp"), 
                      f"{self.fraud_thresholds['rapid_transactions_window']} seconds")
            ) \
            .agg(
                count("*").alias("transaction_count"),
                collect_list("transaction_id").alias("transaction_ids"),
                collect_list("amount").alias("amounts"),
                collect_list("processing_timestamp").alias("timestamps")
            ) \
            .filter(
                col("transaction_count") >= self.fraud_thresholds['rapid_transactions']
            ) \
            .select(
                col("user_id"),
                col("transaction_count"),
                col("transaction_ids"),
                col("amounts"),
                col("timestamps"),
                lit("Rapid transactions").alias("fraud_reason"),
                when(col("transaction_count") >= 5, 0.9)
                .otherwise(0.6).alias("fraud_score")
            )
        
        # Combine fraud detections
        fraud_detected = high_amount_fraud.unionByName(
            rapid_transactions_fraud.select(
                col("user_id"),
                col("transaction_id"),
                col("amount"),
                col("fraud_reason"),
                col("fraud_score"),
                col("processing_timestamp")
            ), allowMissingColumns=True
        )
        
        return fraud_detected
    
    def send_fraud_alerts(self, fraud_df: Any) -> StreamingQuery:
        """Send fraud alerts to Kafka."""
        kafka_config = {
            "kafka.bootstrap.servers": os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
            "topic": os.getenv('KAFKA_TOPIC_FRAUD', 'fraud_alerts')
        }
        
        # Prepare fraud alert data
        fraud_alerts = fraud_df.select(
            col("transaction_id").alias("key"),
            to_json(struct(
                col("transaction_id"),
                col("user_id"),
                col("amount"),
                col("fraud_reason"),
                col("fraud_score"),
                col("processing_timestamp"),
                current_timestamp().alias("alert_timestamp")
            )).alias("value")
        )
        
        return fraud_alerts.writeStream \
            .format("kafka") \
            .options(**kafka_config) \
            .option("checkpointLocation", "./checkpoints/fraud_alerts") \
            .start()
    
    def calculate_live_analytics(self, transactions_df: Any) -> StreamingQuery:
        """Calculate real-time analytics with 1-minute windows."""
        
        # Windowed aggregations
        analytics = transactions_df \
            .withWatermark("processing_timestamp", "1 minute") \
            .groupBy(
                window(col("processing_timestamp"), "1 minute", "30 seconds"),
                col("product_category")
            ) \
            .agg(
                count("*").alias("transaction_count"),
                spark_sum("amount").alias("total_sales"),
                avg("amount").alias("avg_transaction_amount"),
                spark_max("amount").alias("max_transaction_amount"),
                spark_min("amount").alias("min_transaction_amount"),
                countDistinct("user_id").alias("unique_users"),
                countDistinct("product_id").alias("unique_products")
            ) \
            .withColumn(
                "window_start", col("window.start")
            ) \
            .withColumn(
                "window_end", col("window.end")
            ) \
            .drop("window")
        
        # Print analytics to console
        return analytics.writeStream \
            .outputMode("update") \
            .format("console") \
            .option("truncate", False) \
            .option("checkpointLocation", "./checkpoints/analytics") \
            .start()
    
    def save_to_s3_data_lake(self, transactions_df: Any) -> StreamingQuery:
        """Save raw transaction data to S3 data lake in Parquet format."""
        
        # Configure S3 output
        s3_bucket = os.getenv('S3_BUCKET_NAME', 'streamify-analytics-data-lake')
        s3_path = f"s3a://{s3_bucket}/raw_transactions"
        
        return transactions_df.writeStream \
            .format("parquet") \
            .option("path", s3_path) \
            .option("checkpointLocation", "./checkpoints/s3_raw_data") \
            .trigger(processingTime="60 seconds") \
            .start()
    
    def run_stream_processing(self):
        """Run the complete stream processing pipeline."""
        try:
            logger.info("Starting stream processing pipeline...")
            
            # Create Kafka stream for sales data
            sales_stream = self.create_kafka_stream(
                os.getenv('KAFKA_TOPIC_SALES', 'sales_stream')
            )
            
            # Parse transaction data
            transactions_df = self.parse_transaction_data(sales_stream)
            
            # Start fraud detection
            fraud_df = self.detect_fraud(transactions_df)
            fraud_query = self.send_fraud_alerts(fraud_df)
            logger.info("Fraud detection started")
            
            # Start live analytics
            analytics_query = self.calculate_live_analytics(transactions_df)
            logger.info("Live analytics started")
            
            # Start S3 data lake storage
            s3_query = self.save_to_s3_data_lake(transactions_df)
            logger.info("S3 data lake storage started")
            
            # Wait for queries to complete
            fraud_query.awaitTermination()
            analytics_query.awaitTermination()
            s3_query.awaitTermination()
            
        except Exception as e:
            logger.error(f"Error in stream processing: {e}")
            raise
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Clean up Spark resources."""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")

def main():
    """Main function to run stream processing."""
    processor = StreamifyStreamProcessor()
    
    try:
        processor.run_stream_processing()
    except KeyboardInterrupt:
        logger.info("Stream processing stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
