#!/usr/bin/env python3
"""
Pipeline Testing Script
Week 11: Complete Orchestration Pipeline and Testing

This script tests the complete analytics pipeline end-to-end.
"""

import sys
import os
import time
import logging
import json
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config.kafka_config import KafkaConfig
from src.config.aws_config import AWSConfig
from src.config.snowflake_config import SnowflakeConfig
from src.producer.producer import TransactionDataGenerator
from src.processor.data_warehouse_loader import DataWarehouseLoader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PipelineTester:
    """Comprehensive pipeline testing suite."""
    
    def __init__(self):
        self.kafka_config = KafkaConfig()
        self.aws_config = AWSConfig()
        self.snowflake_config = SnowflakeConfig()
        self.test_results = {}
        
    def test_kafka_connectivity(self) -> bool:
        """Test Kafka connectivity and topic creation."""
        try:
            logger.info("Testing Kafka connectivity...")
            
            from kafka import KafkaProducer, KafkaConsumer
            from kafka.admin import KafkaAdminClient, NewTopic
            
            # Test producer
            producer = KafkaProducer(
                bootstrap_servers=[self.kafka_config.bootstrap_servers],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            
            # Test consumer
            consumer = KafkaConsumer(
                self.kafka_config.sales_topic,
                bootstrap_servers=[self.kafka_config.bootstrap_servers],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='test_group'
            )
            
            # Test message production and consumption
            test_message = {
                'test': True,
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
            
            producer.send(self.kafka_config.sales_topic, test_message)
            producer.flush()
            
            # Wait for message
            timeout = 10
            start_time = time.time()
            message_received = False
            
            for message in consumer:
                if time.time() - start_time > timeout:
                    break
                if message.value:
                    received_data = json.loads(message.value.decode('utf-8'))
                    if received_data.get('test'):
                        message_received = True
                        break
            
            producer.close()
            consumer.close()
            
            if message_received:
                logger.info("Kafka connectivity test passed")
                self.test_results['kafka'] = True
                return True
            else:
                logger.error("Kafka message consumption test failed")
                self.test_results['kafka'] = False
                return False
                
        except Exception as e:
            logger.error(f"Kafka connectivity test failed: {e}")
            self.test_results['kafka'] = False
            return False
    
    def test_aws_connectivity(self) -> bool:
        """Test AWS S3 connectivity and permissions."""
        try:
            logger.info("Testing AWS connectivity...")
            
            # Test S3 access
            if not self.aws_config.test_connection():
                logger.error("AWS S3 connectivity test failed")
                self.test_results['aws'] = False
                return False
            
            # Test S3 bucket creation/access
            if not self.aws_config.create_s3_bucket():
                logger.error("S3 bucket creation test failed")
                self.test_results['aws'] = False
                return False
            
            # Test S3 data lake structure
            s3_manager = S3DataLakeManager(self.aws_config)
            s3_manager.create_data_lake_structure()
            
            logger.info("AWS connectivity test passed")
            self.test_results['aws'] = True
            return True
            
        except Exception as e:
            logger.error(f"AWS connectivity test failed: {e}")
            self.test_results['aws'] = False
            return False
    
    def test_snowflake_connectivity(self) -> bool:
        """Test Snowflake connectivity and table creation."""
        try:
            logger.info("Testing Snowflake connectivity...")
            
            # Test connection
            if not self.snowflake_config.test_connection():
                logger.error("Snowflake connectivity test failed")
                self.test_results['snowflake'] = False
                return False
            
            # Test database and schema creation
            if not self.snowflake_config.create_database_and_schema():
                logger.error("Snowflake database creation test failed")
                self.test_results['snowflake'] = False
                return False
            
            # Test table creation
            if not self.snowflake_config.create_tables():
                logger.error("Snowflake table creation test failed")
                self.test_results['snowflake'] = False
                return False
            
            # Test view creation
            if not self.snowflake_config.create_views():
                logger.error("Snowflake view creation test failed")
                self.test_results['snowflake'] = False
                return False
            
            logger.info("Snowflake connectivity test passed")
            self.test_results['snowflake'] = True
            return True
            
        except Exception as e:
            logger.error(f"Snowflake connectivity test failed: {e}")
            self.test_results['snowflake'] = False
            return False
    
    def test_data_generation(self) -> bool:
        """Test data generation process."""
        try:
            logger.info("Testing data generation...")
            
            generator = TransactionDataGenerator()
            
            # Generate a small batch of test data
            test_transactions = []
            for _ in range(10):
                transaction = generator.generate_transaction_data()
                test_transactions.append(transaction)
            
            # Test data structure
            required_fields = [
                'transaction_id', 'user_id', 'amount', 'timestamp',
                'product_category', 'payment_method', 'status'
            ]
            
            for transaction in test_transactions:
                for field in required_fields:
                    if field not in transaction:
                        logger.error(f"Missing required field: {field}")
                        self.test_results['data_generation'] = False
                        return False
            
            # Test data types
            for transaction in test_transactions:
                if not isinstance(transaction['amount'], (int, float)):
                    logger.error("Invalid amount data type")
                    self.test_results['data_generation'] = False
                    return False
                
                if not isinstance(transaction['transaction_id'], str):
                    logger.error("Invalid transaction_id data type")
                    self.test_results['data_generation'] = False
                    return False
            
            generator.close()
            
            logger.info("Data generation test passed")
            self.test_results['data_generation'] = True
            return True
            
        except Exception as e:
            logger.error(f"Data generation test failed: {e}")
            self.test_results['data_generation'] = False
            return False
    
    def test_end_to_end_pipeline(self) -> bool:
        """Test complete end-to-end pipeline."""
        try:
            logger.info("Testing end-to-end pipeline...")
            
            # Start data generation
            generator = TransactionDataGenerator()
            
            # Generate test data
            test_transactions = []
            for _ in range(20):
                transaction = generator.generate_transaction_data()
                test_transactions.append(transaction)
                
                # Send to Kafka
                if not generator.send_transaction(transaction):
                    logger.error("Failed to send transaction to Kafka")
                    self.test_results['end_to_end'] = False
                    return False
            
            generator.close()
            
            # Wait for processing
            logger.info("Waiting for stream processing...")
            time.sleep(30)
            
            # Test data warehouse loading
            loader = DataWarehouseLoader()
            
            # Check if data was loaded
            if not self.snowflake_config.connect():
                logger.error("Failed to connect to Snowflake for verification")
                self.test_results['end_to_end'] = False
                return False
            
            # Check for loaded data
            result = self.snowflake_config.execute_query("""
                SELECT COUNT(*) as count 
                FROM raw_transactions 
                WHERE created_at >= CURRENT_TIMESTAMP() - INTERVAL '1 hour'
            """)
            
            if result and result[0]['COUNT'] > 0:
                logger.info("End-to-end pipeline test passed")
                self.test_results['end_to_end'] = True
                return True
            else:
                logger.error("No data found in data warehouse")
                self.test_results['end_to_end'] = False
                return False
                
        except Exception as e:
            logger.error(f"End-to-end pipeline test failed: {e}")
            self.test_results['end_to_end'] = False
            return False
    
    def test_performance_metrics(self) -> Dict[str, Any]:
        """Test pipeline performance metrics."""
        try:
            logger.info("Testing performance metrics...")
            
            performance_metrics = {
                'data_generation_rate': 0,
                'kafka_throughput': 0,
                'processing_latency': 0,
                'storage_efficiency': 0
            }
            
            # Test data generation rate
            generator = TransactionDataGenerator()
            start_time = time.time()
            
            for _ in range(100):
                generator.generate_transaction_data()
            
            generation_time = time.time() - start_time
            performance_metrics['data_generation_rate'] = 100 / generation_time
            
            generator.close()
            
            # Test Kafka throughput
            from kafka import KafkaProducer
            producer = KafkaProducer(
                bootstrap_servers=[self.kafka_config.bootstrap_servers],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            
            start_time = time.time()
            for i in range(50):
                message = {'test': i, 'timestamp': datetime.now().isoformat()}
                producer.send(self.kafka_config.sales_topic, message)
            
            producer.flush()
            kafka_time = time.time() - start_time
            performance_metrics['kafka_throughput'] = 50 / kafka_time
            
            producer.close()
            
            logger.info(f"Performance metrics: {performance_metrics}")
            return performance_metrics
            
        except Exception as e:
            logger.error(f"Performance test failed: {e}")
            return {}
    
    def run_all_tests(self) -> Dict[str, Any]:
        """Run all pipeline tests."""
        logger.info("Starting comprehensive pipeline testing...")
        
        test_start_time = datetime.now()
        
        # Run individual component tests
        self.test_kafka_connectivity()
        self.test_aws_connectivity()
        self.test_snowflake_connectivity()
        self.test_data_generation()
        
        # Run end-to-end test
        self.test_end_to_end_pipeline()
        
        # Run performance tests
        performance_metrics = self.test_performance_metrics()
        
        test_end_time = datetime.now()
        test_duration = (test_end_time - test_start_time).total_seconds()
        
        # Compile results
        results = {
            'test_timestamp': test_start_time.isoformat(),
            'test_duration_seconds': test_duration,
            'component_tests': self.test_results,
            'performance_metrics': performance_metrics,
            'overall_success': all(self.test_results.values()),
            'summary': {
                'total_tests': len(self.test_results),
                'passed_tests': sum(1 for v in self.test_results.values() if v),
                'failed_tests': sum(1 for v in self.test_results.values() if not v)
            }
        }
        
        # Save results
        results_file = f"/tmp/pipeline_test_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        logger.info(f"Test results saved to: {results_file}")
        
        # Print summary
        logger.info("=== PIPELINE TEST SUMMARY ===")
        logger.info(f"Overall Success: {results['overall_success']}")
        logger.info(f"Tests Passed: {results['summary']['passed_tests']}/{results['summary']['total_tests']}")
        logger.info(f"Test Duration: {test_duration:.2f} seconds")
        
        for component, success in self.test_results.items():
            status = "PASS" if success else "FAIL"
            logger.info(f"{component.upper()}: {status}")
        
        return results

def main():
    """Main function to run pipeline tests."""
    try:
        tester = PipelineTester()
        results = tester.run_all_tests()
        
        if results['overall_success']:
            logger.info("All pipeline tests passed successfully!")
            return 0
        else:
            logger.error("Some pipeline tests failed!")
            return 1
            
    except Exception as e:
        logger.error(f"Pipeline testing failed: {e}")
        return 1

if __name__ == "__main__":
    exit(main())
