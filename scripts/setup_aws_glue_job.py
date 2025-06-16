#!/usr/bin/env python3
"""
AWS Glue Job Setup Script
Week 10: AWS Glue Job Configuration for ELT Processing

This script sets up AWS Glue jobs for the Streamify Analytics Pipeline.
"""

import boto3
import json
import logging
import os
from botocore.exceptions import ClientError
from typing import Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GlueJobSetup:
    """Setup AWS Glue jobs for the analytics pipeline."""
    
    def __init__(self):
        self.region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        self.glue_client = boto3.client('glue', region_name=self.region)
        self.s3_client = boto3.client('s3', region_name=self.region)
        
        # Job configuration
        self.job_name = 'streamify-analytics-etl-job'
        self.role_name = 'GlueServiceRole-StreamifyAnalytics'
        self.script_location = f"s3://{os.getenv('S3_BUCKET_NAME', 'streamify-analytics-data-lake')}/scripts/aws_glue_job.py"
        
    def create_glue_role(self) -> bool:
        """Create IAM role for Glue job."""
        try:
            iam_client = boto3.client('iam', region_name=self.region)
            
            # Trust policy for Glue
            trust_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "glue.amazonaws.com"
                        },
                        "Action": "sts:AssumeRole"
                    }
                ]
            }
            
            # Create role
            try:
                iam_client.create_role(
                    RoleName=self.role_name,
                    AssumeRolePolicyDocument=json.dumps(trust_policy),
                    Description='Service role for Streamify Analytics Glue jobs'
                )
                logger.info(f"Created IAM role: {self.role_name}")
            except ClientError as e:
                if e.response['Error']['Code'] == 'EntityAlreadyExists':
                    logger.info(f"IAM role {self.role_name} already exists")
                else:
                    raise
            
            # Attach necessary policies
            policies = [
                'arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole',
                'arn:aws:iam::aws:policy/AmazonS3FullAccess',
                'arn:aws:iam::aws:policy/CloudWatchLogsFullAccess'
            ]
            
            for policy_arn in policies:
                try:
                    iam_client.attach_role_policy(
                        RoleName=self.role_name,
                        PolicyArn=policy_arn
                    )
                    logger.info(f"Attached policy: {policy_arn}")
                except ClientError as e:
                    if e.response['Error']['Code'] != 'EntityAlreadyExists':
                        logger.warning(f"Failed to attach policy {policy_arn}: {e}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error creating Glue role: {e}")
            return False
    
    def upload_glue_script(self) -> bool:
        """Upload Glue job script to S3."""
        try:
            script_path = 'scripts/aws_glue_job.py'
            s3_key = f"scripts/{os.path.basename(script_path)}"
            bucket_name = os.getenv('S3_BUCKET_NAME', 'streamify-analytics-data-lake')
            
            # Upload script
            self.s3_client.upload_file(script_path, bucket_name, s3_key)
            logger.info(f"Uploaded Glue script to s3://{bucket_name}/{s3_key}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error uploading Glue script: {e}")
            return False
    
    def create_glue_job(self) -> bool:
        """Create Glue job for ETL processing."""
        try:
            job_config = {
                'Name': self.job_name,
                'Role': f"arn:aws:iam::{boto3.client('sts').get_caller_identity()['Account']}:role/{self.role_name}",
                'Command': {
                    'Name': 'glueetl',
                    'ScriptLocation': self.script_location,
                    'PythonVersion': '3'
                },
                'DefaultArguments': {
                    '--job-language': 'python',
                    '--job-bookmark-option': 'job-bookmark-enable',
                    '--enable-metrics': 'true',
                    '--enable-continuous-cloudwatch-log': 'true',
                    '--enable-spark-ui': 'true',
                    '--spark-event-logs-path': f"s3://{os.getenv('S3_BUCKET_NAME', 'streamify-analytics-data-lake')}/spark-logs/",
                    '--TempDir': f"s3://{os.getenv('S3_BUCKET_NAME', 'streamify-analytics-data-lake')}/temp/",
                    '--S3_INPUT_PATH': f"s3://{os.getenv('S3_BUCKET_NAME', 'streamify-analytics-data-lake')}/raw_transactions/",
                    '--S3_OUTPUT_PATH': f"s3://{os.getenv('S3_BUCKET_NAME', 'streamify-analytics-data-lake')}/processed_transactions/",
                    '--SNOWFLAKE_ACCOUNT': os.getenv('SNOWFLAKE_ACCOUNT', ''),
                    '--SNOWFLAKE_USER': os.getenv('SNOWFLAKE_USER', ''),
                    '--SNOWFLAKE_PASSWORD': os.getenv('SNOWFLAKE_PASSWORD', ''),
                    '--SNOWFLAKE_WAREHOUSE': os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
                    '--SNOWFLAKE_DATABASE': os.getenv('SNOWFLAKE_DATABASE', 'STREAMIFY_ANALYTICS'),
                    '--SNOWFLAKE_SCHEMA': os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')
                },
                'MaxCapacity': 2,
                'Timeout': 60,
                'GlueVersion': '3.0',
                'WorkerType': 'G.1X',
                'NumberOfWorkers': 2,
                'Tags': {
                    'Project': 'StreamifyAnalytics',
                    'Environment': 'Production',
                    'Owner': 'DataEngineering'
                }
            }
            
            try:
                response = self.glue_client.create_job(**job_config)
                logger.info(f"Created Glue job: {self.job_name}")
                return True
            except ClientError as e:
                if e.response['Error']['Code'] == 'AlreadyExistsException':
                    logger.info(f"Glue job {self.job_name} already exists")
                    return True
                else:
                    raise
                    
        except Exception as e:
            logger.error(f"Error creating Glue job: {e}")
            return False
    
    def create_glue_crawler(self) -> bool:
        """Create Glue crawler for S3 data discovery."""
        try:
            crawler_name = 'streamify-analytics-crawler'
            s3_path = f"s3://{os.getenv('S3_BUCKET_NAME', 'streamify-analytics-data-lake')}/raw_transactions/"
            
            crawler_config = {
                'Name': crawler_name,
                'Role': f"arn:aws:iam::{boto3.client('sts').get_caller_identity()['Account']}:role/{self.role_name}",
                'DatabaseName': 'streamify_analytics_raw',
                'Targets': {
                    'S3Targets': [
                        {
                            'Path': s3_path,
                            'Exclusions': ['**/.*']
                        }
                    ]
                },
                'SchemaChangePolicy': {
                    'UpdateBehavior': 'UPDATE_IN_DATABASE',
                    'DeleteBehavior': 'LOG'
                },
                'RecrawlPolicy': {
                    'RecrawlBehavior': 'CRAWL_EVERYTHING'
                },
                'Tags': {
                    'Project': 'StreamifyAnalytics',
                    'Environment': 'Production'
                }
            }
            
            try:
                self.glue_client.create_crawler(**crawler_config)
                logger.info(f"Created Glue crawler: {crawler_name}")
                return True
            except ClientError as e:
                if e.response['Error']['Code'] == 'AlreadyExistsException':
                    logger.info(f"Glue crawler {crawler_name} already exists")
                    return True
                else:
                    raise
                    
        except Exception as e:
            logger.error(f"Error creating Glue crawler: {e}")
            return False
    
    def create_glue_database(self) -> bool:
        """Create Glue database for cataloging data."""
        try:
            database_name = 'streamify_analytics_raw'
            
            database_config = {
                'Name': database_name,
                'Description': 'Database for Streamify Analytics raw data',
                'Parameters': {
                    'classification': 'parquet'
                }
            }
            
            try:
                self.glue_client.create_database(**database_config)
                logger.info(f"Created Glue database: {database_name}")
                return True
            except ClientError as e:
                if e.response['Error']['Code'] == 'AlreadyExistsException':
                    logger.info(f"Glue database {database_name} already exists")
                    return True
                else:
                    raise
                    
        except Exception as e:
            logger.error(f"Error creating Glue database: {e}")
            return False
    
    def test_glue_job(self) -> bool:
        """Test the Glue job execution."""
        try:
            logger.info(f"Testing Glue job: {self.job_name}")
            
            # Start job run
            response = self.glue_client.start_job_run(
                JobName=self.job_name,
                Arguments={
                    '--S3_INPUT_PATH': f"s3://{os.getenv('S3_BUCKET_NAME', 'streamify-analytics-data-lake')}/raw_transactions/",
                    '--S3_OUTPUT_PATH': f"s3://{os.getenv('S3_BUCKET_NAME', 'streamify-analytics-data-lake')}/processed_transactions/"
                }
            )
            
            job_run_id = response['JobRunId']
            logger.info(f"Started Glue job run: {job_run_id}")
            
            # Wait for job completion
            import time
            max_wait_time = 600  # 10 minutes
            wait_time = 0
            
            while wait_time < max_wait_time:
                job_run = self.glue_client.get_job_run(
                    JobName=self.job_name,
                    RunId=job_run_id
                )
                
                status = job_run['JobRun']['JobRunState']
                logger.info(f"Job status: {status}")
                
                if status in ['SUCCEEDED', 'FAILED', 'STOPPED', 'TIMEOUT']:
                    break
                
                time.sleep(30)
                wait_time += 30
            
            if status == 'SUCCEEDED':
                logger.info("Glue job test completed successfully")
                return True
            else:
                logger.error(f"Glue job test failed with status: {status}")
                return False
                
        except Exception as e:
            logger.error(f"Error testing Glue job: {e}")
            return False
    
    def setup_complete_pipeline(self) -> bool:
        """Set up the complete Glue pipeline."""
        try:
            logger.info("Setting up complete Glue pipeline...")
            
            # Create IAM role
            if not self.create_glue_role():
                return False
            
            # Create Glue database
            if not self.create_glue_database():
                return False
            
            # Upload Glue script
            if not self.upload_glue_script():
                return False
            
            # Create Glue job
            if not self.create_glue_job():
                return False
            
            # Create Glue crawler
            if not self.create_glue_crawler():
                return False
            
            logger.info("Glue pipeline setup completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error setting up Glue pipeline: {e}")
            return False

def main():
    """Main function to set up Glue jobs."""
    try:
        setup = GlueJobSetup()
        
        # Set up complete pipeline
        if setup.setup_complete_pipeline():
            logger.info("AWS Glue setup completed successfully!")
            
            # Optionally test the job
            test_job = input("Would you like to test the Glue job? (y/n): ").lower().strip()
            if test_job == 'y':
                setup.test_glue_job()
        else:
            logger.error("AWS Glue setup failed!")
            return 1
        
        return 0
        
    except Exception as e:
        logger.error(f"Setup failed: {e}")
        return 1

if __name__ == "__main__":
    exit(main())
