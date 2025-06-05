"""
AWS Configuration Module
Week 6-8: Data Lake & Warehousing

This module handles AWS configuration and S3 data lake integration.
"""

import os
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from typing import Dict, Any, Optional
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

class AWSConfig:
    """AWS configuration and client management."""
    
    def __init__(self):
        self.region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        self.s3_bucket = os.getenv('S3_BUCKET_NAME', 'streamify-analytics-data-lake')
        
        # Initialize AWS clients
        self.s3_client = None
        self.s3_resource = None
        self.glue_client = None
        
        self._initialize_clients()
    
    def _initialize_clients(self):
        """Initialize AWS service clients."""
        try:
            # Initialize S3 client
            self.s3_client = boto3.client(
                's3',
                region_name=self.region,
                aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
            )
            
            # Initialize S3 resource
            self.s3_resource = boto3.resource(
                's3',
                region_name=self.region,
                aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
            )
            
            # Initialize Glue client
            self.glue_client = boto3.client(
                'glue',
                region_name=self.region,
                aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
            )
            
            logger.info("AWS clients initialized successfully")
            
        except NoCredentialsError:
            logger.error("AWS credentials not found. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
            raise
        except Exception as e:
            logger.error(f"Failed to initialize AWS clients: {e}")
            raise
    
    def test_connection(self) -> bool:
        """Test AWS connection and permissions."""
        try:
            # Test S3 access
            self.s3_client.head_bucket(Bucket=self.s3_bucket)
            logger.info(f"S3 bucket '{self.s3_bucket}' is accessible")
            
            # Test Glue access
            self.glue_client.list_jobs()
            logger.info("Glue service is accessible")
            
            return True
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                logger.error(f"S3 bucket '{self.s3_bucket}' not found")
            elif error_code == '403':
                logger.error(f"Access denied to S3 bucket '{self.s3_bucket}'")
            else:
                logger.error(f"S3 error: {e}")
            return False
        except Exception as e:
            logger.error(f"Error testing AWS connection: {e}")
            return False
    
    def create_s3_bucket(self) -> bool:
        """Create S3 bucket if it doesn't exist."""
        try:
            # Check if bucket exists
            self.s3_client.head_bucket(Bucket=self.s3_bucket)
            logger.info(f"S3 bucket '{self.s3_bucket}' already exists")
            return True
            
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                # Bucket doesn't exist, create it
                try:
                    if self.region == 'us-east-1':
                        # us-east-1 doesn't need LocationConstraint
                        self.s3_client.create_bucket(Bucket=self.s3_bucket)
                    else:
                        self.s3_client.create_bucket(
                            Bucket=self.s3_bucket,
                            CreateBucketConfiguration={'LocationConstraint': self.region}
                        )
                    
                    logger.info(f"S3 bucket '{self.s3_bucket}' created successfully")
                    
                    # Set up bucket policies and configurations
                    self._configure_s3_bucket()
                    return True
                    
                except ClientError as create_error:
                    logger.error(f"Failed to create S3 bucket: {create_error}")
                    return False
            else:
                logger.error(f"Error checking S3 bucket: {e}")
                return False
    
    def _configure_s3_bucket(self):
        """Configure S3 bucket with appropriate settings."""
        try:
            # Enable versioning
            self.s3_client.put_bucket_versioning(
                Bucket=self.s3_bucket,
                VersioningConfiguration={'Status': 'Enabled'}
            )
            
            # Set up lifecycle policy
            lifecycle_config = {
                'Rules': [
                    {
                        'ID': 'DeleteOldVersions',
                        'Status': 'Enabled',
                        'Filter': {'Prefix': ''},
                        'NoncurrentVersionExpiration': {'NoncurrentDays': 30}
                    },
                    {
                        'ID': 'ArchiveOldData',
                        'Status': 'Enabled',
                        'Filter': {'Prefix': 'raw_transactions/'},
                        'Transitions': [
                            {
                                'Days': 30,
                                'StorageClass': 'STANDARD_IA'
                            },
                            {
                                'Days': 90,
                                'StorageClass': 'GLACIER'
                            }
                        ]
                    }
                ]
            }
            
            self.s3_client.put_bucket_lifecycle_configuration(
                Bucket=self.s3_bucket,
                LifecycleConfiguration=lifecycle_config
            )
            
            logger.info("S3 bucket configured with versioning and lifecycle policies")
            
        except Exception as e:
            logger.warning(f"Failed to configure S3 bucket: {e}")
    
    def get_s3_path(self, data_type: str, timestamp: Optional[datetime] = None) -> str:
        """Generate S3 path for data storage."""
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)
        
        year = timestamp.strftime('%Y')
        month = timestamp.strftime('%m')
        day = timestamp.strftime('%d')
        hour = timestamp.strftime('%H')
        
        return f"s3://{self.s3_bucket}/{data_type}/year={year}/month={month}/day={day}/hour={hour}/"
    
    def list_s3_objects(self, prefix: str, max_keys: int = 1000) -> list:
        """List objects in S3 bucket with given prefix."""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.s3_bucket,
                Prefix=prefix,
                MaxKeys=max_keys
            )
            
            return response.get('Contents', [])
            
        except ClientError as e:
            logger.error(f"Error listing S3 objects: {e}")
            return []
    
    def upload_to_s3(self, file_path: str, s3_key: str, 
                    metadata: Optional[Dict[str, str]] = None) -> bool:
        """Upload file to S3."""
        try:
            extra_args = {}
            if metadata:
                extra_args['Metadata'] = metadata
            
            self.s3_client.upload_file(
                file_path,
                self.s3_bucket,
                s3_key,
                ExtraArgs=extra_args
            )
            
            logger.info(f"File uploaded to S3: s3://{self.s3_bucket}/{s3_key}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to upload file to S3: {e}")
            return False
    
    def download_from_s3(self, s3_key: str, local_path: str) -> bool:
        """Download file from S3."""
        try:
            self.s3_client.download_file(
                self.s3_bucket,
                s3_key,
                local_path
            )
            
            logger.info(f"File downloaded from S3: {s3_key} -> {local_path}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to download file from S3: {e}")
            return False
    
    def delete_s3_object(self, s3_key: str) -> bool:
        """Delete object from S3."""
        try:
            self.s3_client.delete_object(
                Bucket=self.s3_bucket,
                Key=s3_key
            )
            
            logger.info(f"Object deleted from S3: {s3_key}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to delete object from S3: {e}")
            return False

class S3DataLakeManager:
    """Manages data lake operations in S3."""
    
    def __init__(self, aws_config: AWSConfig):
        self.aws_config = aws_config
        self.s3_client = aws_config.s3_client
        self.s3_bucket = aws_config.s3_bucket
    
    def create_data_lake_structure(self):
        """Create the data lake directory structure."""
        directories = [
            'raw_transactions/',
            'processed_transactions/',
            'fraud_alerts/',
            'analytics_results/',
            'data_quality_reports/',
            'backup/'
        ]
        
        for directory in directories:
            # Create empty object to represent directory
            s3_key = f"{directory}.gitkeep"
            try:
                self.s3_client.put_object(
                    Bucket=self.s3_bucket,
                    Key=s3_key,
                    Body=b''
                )
                logger.info(f"Created directory: {directory}")
            except ClientError as e:
                logger.error(f"Failed to create directory {directory}: {e}")
    
    def get_data_lake_summary(self) -> Dict[str, Any]:
        """Get summary of data lake contents."""
        summary = {
            'total_objects': 0,
            'total_size_bytes': 0,
            'directories': {},
            'last_updated': None
        }
        
        try:
            # List all objects
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.s3_bucket)
            
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        summary['total_objects'] += 1
                        summary['total_size_bytes'] += obj['Size']
                        
                        # Track by directory
                        key_parts = obj['Key'].split('/')
                        if len(key_parts) > 1:
                            directory = key_parts[0] + '/'
                            if directory not in summary['directories']:
                                summary['directories'][directory] = {
                                    'object_count': 0,
                                    'total_size_bytes': 0
                                }
                            summary['directories'][directory]['object_count'] += 1
                            summary['directories'][directory]['total_size_bytes'] += obj['Size']
                        
                        # Track last updated
                        if summary['last_updated'] is None or obj['LastModified'] > summary['last_updated']:
                            summary['last_updated'] = obj['LastModified']
            
            # Convert bytes to human readable format
            summary['total_size_mb'] = summary['total_size_bytes'] / (1024 * 1024)
            summary['total_size_gb'] = summary['total_size_bytes'] / (1024 * 1024 * 1024)
            
            for directory in summary['directories']:
                dir_data = summary['directories'][directory]
                dir_data['total_size_mb'] = dir_data['total_size_bytes'] / (1024 * 1024)
                dir_data['total_size_gb'] = dir_data['total_size_bytes'] / (1024 * 1024 * 1024)
            
        except Exception as e:
            logger.error(f"Error getting data lake summary: {e}")
        
        return summary
