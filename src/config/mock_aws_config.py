"""
Mock AWS Configuration for Testing
This simulates AWS services without actual cloud costs
"""

import json
import os
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List
import tempfile

logger = logging.getLogger(__name__)

class MockAWSConfig:
    """Mock AWS configuration for local testing without cloud costs."""
    
    def __init__(self):
        self.region = 'us-east-1'
        self.s3_bucket = 'mock-streamify-analytics-data-lake'
        
        # Create local mock storage
        self.mock_storage_dir = tempfile.mkdtemp(prefix='mock_s3_')
        self._create_mock_structure()
        
        logger.info(f"Mock AWS initialized with storage at: {self.mock_storage_dir}")
    
    def _create_mock_structure(self):
        """Create mock S3 directory structure."""
        directories = [
            'raw_transactions/',
            'processed_transactions/',
            'fraud_alerts/',
            'analytics_results/',
            'data_quality_reports/',
            'backup/'
        ]
        
        for directory in directories:
            os.makedirs(os.path.join(self.mock_storage_dir, directory), exist_ok=True)
    
    def test_connection(self) -> bool:
        """Mock connection test - always passes."""
        logger.info("Mock AWS connection test passed")
        return True
    
    def create_s3_bucket(self) -> bool:
        """Mock bucket creation - always succeeds."""
        logger.info(f"Mock S3 bucket '{self.s3_bucket}' created")
        return True
    
    def get_s3_path(self, data_type: str, timestamp: datetime = None) -> str:
        """Generate mock S3 path."""
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)
        
        year = timestamp.strftime('%Y')
        month = timestamp.strftime('%m')
        day = timestamp.strftime('%d')
        hour = timestamp.strftime('%H')
        
        return f"{self.mock_storage_dir}/{data_type}/year={year}/month={month}/day={day}/hour={hour}/"
    
    def list_s3_objects(self, prefix: str, max_keys: int = 1000) -> list:
        """Mock S3 object listing."""
        objects = []
        prefix_path = os.path.join(self.mock_storage_dir, prefix)
        
        if os.path.exists(prefix_path):
            for root, dirs, files in os.walk(prefix_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    rel_path = os.path.relpath(file_path, self.mock_storage_dir)
                    stat = os.stat(file_path)
                    
                    objects.append({
                        'Key': rel_path,
                        'Size': stat.st_size,
                        'LastModified': datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
                    })
        
        return objects[:max_keys]
    
    def upload_to_s3(self, file_path: str, s3_key: str, metadata: Dict[str, str] = None) -> bool:
        """Mock S3 upload."""
        try:
            dest_path = os.path.join(self.mock_storage_dir, s3_key)
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            
            with open(file_path, 'rb') as src, open(dest_path, 'wb') as dst:
                dst.write(src.read())
            
            logger.info(f"Mock upload: {file_path} -> {s3_key}")
            return True
        except Exception as e:
            logger.error(f"Mock upload failed: {e}")
            return False
    
    def download_from_s3(self, s3_key: str, local_path: str) -> bool:
        """Mock S3 download."""
        try:
            src_path = os.path.join(self.mock_storage_dir, s3_key)
            
            if not os.path.exists(src_path):
                logger.warning(f"Mock file not found: {s3_key}")
                return False
            
            with open(src_path, 'rb') as src, open(local_path, 'wb') as dst:
                dst.write(src.read())
            
            logger.info(f"Mock download: {s3_key} -> {local_path}")
            return True
        except Exception as e:
            logger.error(f"Mock download failed: {e}")
            return False
    
    def delete_s3_object(self, s3_key: str) -> bool:
        """Mock S3 object deletion."""
        try:
            file_path = os.path.join(self.mock_storage_dir, s3_key)
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Mock delete: {s3_key}")
                return True
            return False
        except Exception as e:
            logger.error(f"Mock delete failed: {e}")
            return False

class MockS3DataLakeManager:
    """Mock S3 data lake manager for testing."""
    
    def __init__(self, aws_config: MockAWSConfig):
        self.aws_config = aws_config
        self.mock_storage_dir = aws_config.mock_storage_dir
    
    def create_data_lake_structure(self):
        """Create mock data lake structure."""
        logger.info("Mock data lake structure created")
    
    def get_data_lake_summary(self) -> Dict[str, Any]:
        """Get mock data lake summary."""
        total_objects = 0
        total_size = 0
        
        for root, dirs, files in os.walk(self.mock_storage_dir):
            for file in files:
                file_path = os.path.join(root, file)
                total_objects += 1
                total_size += os.path.getsize(file_path)
        
        return {
            'total_objects': total_objects,
            'total_size_bytes': total_size,
            'total_size_mb': total_size / (1024 * 1024),
            'total_size_gb': total_size / (1024 * 1024 * 1024),
            'mock_storage_path': self.mock_storage_dir
        }
