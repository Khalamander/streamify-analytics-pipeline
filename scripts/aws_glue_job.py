#!/usr/bin/env python3
"""
AWS Glue Job for ELT Processing
Week 10: AWS Glue Job Configuration for ELT Processing

This script performs Extract, Load, Transform (ELT) operations
to process data from S3 data lake to Snowflake data warehouse.
"""

import sys
import os
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List

# AWS Glue imports
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StreamifyGlueETLJob:
    """AWS Glue ETL job for Streamify Analytics Pipeline."""
    
    def __init__(self):
        # Initialize Glue context
        self.sc = SparkContext()
        self.glueContext = GlueContext(self.sc)
        self.spark = self.glueContext.spark_session
        
        # Get job parameters
        self.args = getResolvedOptions(sys.argv, [
            'JOB_NAME',
            'S3_INPUT_PATH',
            'S3_OUTPUT_PATH',
            'SNOWFLAKE_ACCOUNT',
            'SNOWFLAKE_USER',
            'SNOWFLAKE_PASSWORD',
            'SNOWFLAKE_WAREHOUSE',
            'SNOWFLAKE_DATABASE',
            'SNOWFLAKE_SCHEMA'
        ])
        
        # Initialize job
        self.job = Job(self.glueContext)
        self.job.init(self.args['JOB_NAME'], self.args)
        
        logger.info(f"Initialized Glue job: {self.args['JOB_NAME']}")
    
    def extract_raw_transactions(self) -> DataFrame:
        """Extract raw transaction data from S3."""
        try:
            logger.info("Extracting raw transaction data from S3...")
            
            # Read Parquet files from S3
            input_path = self.args['S3_INPUT_PATH']
            raw_df = self.spark.read.parquet(input_path)
            
            logger.info(f"Extracted {raw_df.count()} raw transaction records")
            return raw_df
            
        except Exception as e:
            logger.error(f"Error extracting raw transactions: {e}")
            raise
    
    def transform_transactions(self, raw_df: DataFrame) -> DataFrame:
        """Transform raw transaction data."""
        try:
            logger.info("Transforming transaction data...")
            
            # Data cleaning and transformation
            transformed_df = raw_df.select(
                col("transaction_id"),
                col("user_id"),
                col("user_email"),
                col("user_name"),
                col("product_id"),
                col("product_name"),
                col("product_category"),
                col("amount").cast(DecimalType(10, 2)).alias("amount"),
                col("currency"),
                col("payment_method"),
                col("status"),
                to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("transaction_timestamp"),
                col("location"),
                col("behavior_pattern"),
                col("session_id"),
                col("device_type"),
                col("browser"),
                col("ip_address"),
                current_timestamp().alias("processing_timestamp")
            ).filter(
                col("transaction_id").isNotNull() &
                col("amount").isNotNull() &
                (col("amount") > 0)
            )
            
            # Add data quality flags
            transformed_df = transformed_df.withColumn(
                "data_quality_score",
                when(col("amount") > 10000, 0.8)
                .when(col("amount") < 0.01, 0.6)
                .otherwise(1.0)
            ).withColumn(
                "is_valid_email",
                col("user_email").rlike("^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$")
            )
            
            logger.info(f"Transformed {transformed_df.count()} transaction records")
            return transformed_df
            
        except Exception as e:
            logger.error(f"Error transforming transactions: {e}")
            raise
    
    def calculate_aggregations(self, df: DataFrame) -> Dict[str, DataFrame]:
        """Calculate various aggregations for analytics."""
        try:
            logger.info("Calculating aggregations...")
            
            # Daily sales summary
            daily_sales = df.groupBy(
                date_format(col("transaction_timestamp"), "yyyy-MM-dd").alias("sale_date")
            ).agg(
                count("*").alias("total_transactions"),
                sum("amount").alias("total_revenue"),
                avg("amount").alias("avg_transaction_value"),
                countDistinct("user_id").alias("unique_customers"),
                countDistinct("product_category").alias("categories_sold"),
                sum(when(col("status") == "completed", 1).otherwise(0)).alias("completed_transactions"),
                sum(when(col("status") == "failed", 1).otherwise(0)).alias("failed_transactions")
            )
            
            # Product performance
            product_performance = df.filter(col("status") == "completed").groupBy(
                "product_category", "product_name"
            ).agg(
                count("*").alias("total_sales"),
                sum("amount").alias("total_revenue"),
                avg("amount").alias("avg_price"),
                countDistinct("user_id").alias("unique_customers")
            ).withColumn(
                "category_rank",
                rank().over(Window.partitionBy("product_category").orderBy(desc("total_revenue")))
            )
            
            # User behavior analysis
            user_behavior = df.groupBy("user_id").agg(
                count("*").alias("total_transactions"),
                sum("amount").alias("total_spent"),
                avg("amount").alias("avg_transaction_amount"),
                countDistinct("product_category").alias("categories_purchased"),
                countDistinct("session_id").alias("unique_sessions"),
                min("transaction_timestamp").alias("first_transaction"),
                max("transaction_timestamp").alias("last_transaction")
            ).withColumn(
                "customer_lifetime_value",
                col("total_spent")
            ).withColumn(
                "transaction_frequency",
                col("total_transactions") / 
                (datediff(col("last_transaction"), col("first_transaction")) + 1)
            )
            
            # Geographic analysis
            geo_analysis = df.groupBy(
                col("location.country").alias("country"),
                col("location.state").alias("state")
            ).agg(
                count("*").alias("transaction_count"),
                sum("amount").alias("total_revenue"),
                countDistinct("user_id").alias("unique_users"),
                avg("amount").alias("avg_transaction_amount")
            )
            
            # Device and browser analysis
            device_analysis = df.groupBy("device_type", "browser").agg(
                count("*").alias("transaction_count"),
                sum("amount").alias("total_revenue"),
                countDistinct("user_id").alias("unique_users"),
                avg("amount").alias("avg_transaction_amount")
            )
            
            aggregations = {
                "daily_sales": daily_sales,
                "product_performance": product_performance,
                "user_behavior": user_behavior,
                "geo_analysis": geo_analysis,
                "device_analysis": device_analysis
            }
            
            logger.info("Aggregations calculated successfully")
            return aggregations
            
        except Exception as e:
            logger.error(f"Error calculating aggregations: {e}")
            raise
    
    def load_to_snowflake(self, df: DataFrame, table_name: str):
        """Load DataFrame to Snowflake."""
        try:
            logger.info(f"Loading data to Snowflake table: {table_name}")
            
            # Snowflake connection properties
            snowflake_options = {
                "sfURL": f"{self.args['SNOWFLAKE_ACCOUNT']}.snowflakecomputing.com",
                "sfUser": self.args['SNOWFLAKE_USER'],
                "sfPassword": self.args['SNOWFLAKE_PASSWORD'],
                "sfDatabase": self.args['SNOWFLAKE_DATABASE'],
                "sfSchema": self.args['SNOWFLAKE_SCHEMA'],
                "sfWarehouse": self.args['SNOWFLAKE_WAREHOUSE']
            }
            
            # Write to Snowflake
            df.write \
                .format("snowflake") \
                .options(**snowflake_options) \
                .option("dbtable", table_name) \
                .mode("append") \
                .save()
            
            logger.info(f"Successfully loaded data to {table_name}")
            
        except Exception as e:
            logger.error(f"Error loading to Snowflake: {e}")
            raise
    
    def save_to_s3(self, df: DataFrame, output_path: str, table_name: str):
        """Save DataFrame to S3 in Parquet format."""
        try:
            logger.info(f"Saving {table_name} to S3: {output_path}")
            
            # Add partitioning by date
            df_with_partition = df.withColumn(
                "year", year(col("transaction_timestamp"))
            ).withColumn(
                "month", month(col("transaction_timestamp"))
            ).withColumn(
                "day", dayofmonth(col("transaction_timestamp"))
            )
            
            # Write to S3
            df_with_partition.write \
                .partitionBy("year", "month", "day") \
                .mode("append") \
                .parquet(f"{output_path}/{table_name}")
            
            logger.info(f"Successfully saved {table_name} to S3")
            
        except Exception as e:
            logger.error(f"Error saving to S3: {e}")
            raise
    
    def run_etl_pipeline(self):
        """Run the complete ETL pipeline."""
        try:
            logger.info("Starting ETL pipeline...")
            
            # Extract raw data
            raw_df = self.extract_raw_transactions()
            
            # Transform data
            transformed_df = self.transform_transactions(raw_df)
            
            # Load transformed data to Snowflake
            self.load_to_snowflake(transformed_df, "raw_transactions")
            
            # Calculate aggregations
            aggregations = self.calculate_aggregations(transformed_df)
            
            # Load aggregations to Snowflake
            for table_name, df in aggregations.items():
                self.load_to_snowflake(df, table_name)
            
            # Save processed data to S3
            output_path = self.args['S3_OUTPUT_PATH']
            self.save_to_s3(transformed_df, output_path, "processed_transactions")
            
            for table_name, df in aggregations.items():
                self.save_to_s3(df, output_path, f"analytics_{table_name}")
            
            logger.info("ETL pipeline completed successfully")
            
        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}")
            raise
        finally:
            self.job.commit()
    
    def run_data_quality_checks(self, df: DataFrame) -> Dict[str, Any]:
        """Run data quality checks on the dataset."""
        try:
            logger.info("Running data quality checks...")
            
            total_records = df.count()
            
            # Check for null values
            null_checks = {
                "null_transaction_ids": df.filter(col("transaction_id").isNull()).count(),
                "null_amounts": df.filter(col("amount").isNull()).count(),
                "null_user_ids": df.filter(col("user_id").isNull()).count(),
                "null_timestamps": df.filter(col("transaction_timestamp").isNull()).count()
            }
            
            # Check for data anomalies
            anomaly_checks = {
                "negative_amounts": df.filter(col("amount") < 0).count(),
                "zero_amounts": df.filter(col("amount") == 0).count(),
                "very_high_amounts": df.filter(col("amount") > 100000).count(),
                "invalid_emails": df.filter(~col("is_valid_email")).count()
            }
            
            # Calculate data quality score
            quality_score = 1.0
            for check, count in {**null_checks, **anomaly_checks}.items():
                if count > 0:
                    quality_score -= (count / total_records) * 0.1
            
            quality_score = max(0.0, quality_score)
            
            quality_report = {
                "total_records": total_records,
                "quality_score": quality_score,
                "null_checks": null_checks,
                "anomaly_checks": anomaly_checks,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            logger.info(f"Data quality score: {quality_score:.2f}")
            return quality_report
            
        except Exception as e:
            logger.error(f"Error in data quality checks: {e}")
            raise

def main():
    """Main function to run the Glue job."""
    try:
        # Initialize ETL job
        etl_job = StreamifyGlueETLJob()
        
        # Run ETL pipeline
        etl_job.run_etl_pipeline()
        
        logger.info("Glue job completed successfully")
        
    except Exception as e:
        logger.error(f"Glue job failed: {e}")
        raise

if __name__ == "__main__":
    main()
