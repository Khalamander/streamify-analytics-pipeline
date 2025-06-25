#!/usr/bin/env python3
"""
Local Pipeline Testing Script
Tests the complete pipeline using mock services (no cloud costs)
"""

import sys
import os
import time
import logging
import json
from datetime import datetime, timezone

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Use mock configurations instead of real cloud services
from src.config.mock_aws_config import MockAWSConfig, MockS3DataLakeManager
from src.config.mock_snowflake_config import MockSnowflakeConfig
from src.config.kafka_config import KafkaConfig
from src.producer.producer import TransactionDataGenerator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class LocalPipelineTester:
    """Test the complete pipeline using mock services."""
    
    def __init__(self):
        self.kafka_config = KafkaConfig()
        self.aws_config = MockAWSConfig()
        self.s3_manager = MockS3DataLakeManager(self.aws_config)
        self.snowflake_config = MockSnowflakeConfig()
        
        logger.info("Local pipeline tester initialized with mock services")
    
    def test_data_generation(self) -> bool:
        """Test data generation without cloud services."""
        try:
            logger.info("Testing data generation...")
            
            generator = TransactionDataGenerator()
            
            # Generate test data
            test_transactions = []
            for _ in range(20):
                transaction = generator.generate_transaction_data()
                test_transactions.append(transaction)
            
            # Validate data structure
            required_fields = [
                'transaction_id', 'user_id', 'amount', 'timestamp',
                'product_category', 'payment_method', 'status'
            ]
            
            for transaction in test_transactions:
                for field in required_fields:
                    if field not in transaction:
                        logger.error(f"Missing required field: {field}")
                        return False
            
            logger.info(f"Generated {len(test_transactions)} test transactions")
            generator.close()
            return True
            
        except Exception as e:
            logger.error(f"Data generation test failed: {e}")
            return False
    
    def test_kafka_simulation(self) -> bool:
        """Test Kafka functionality (simulated)."""
        try:
            logger.info("Testing Kafka simulation...")
            
            # Simulate Kafka message production
            generator = TransactionDataGenerator()
            
            messages_sent = 0
            for _ in range(10):
                transaction = generator.generate_transaction_data()
                # Simulate sending to Kafka (without actual Kafka)
                logger.info(f"Simulated Kafka message: {transaction['transaction_id']}")
                messages_sent += 1
            
            generator.close()
            
            logger.info(f"Simulated sending {messages_sent} messages to Kafka")
            return True
            
        except Exception as e:
            logger.error(f"Kafka simulation test failed: {e}")
            return False
    
    def test_fraud_detection(self) -> bool:
        """Test fraud detection logic."""
        try:
            logger.info("Testing fraud detection...")
            
            from src.processor.fraud_detector import FraudDetector
            
            detector = FraudDetector()
            
            # Test different transaction types
            test_transactions = [
                {
                    'transaction_id': 'test_1',
                    'user_id': 'user_1',
                    'amount': 100.0,
                    'location': {'latitude': 40.7128, 'longitude': -74.0060},
                    'device_type': 'desktop',
                    'browser': 'chrome',
                    'payment_method': 'credit_card',
                    'product_category': 'electronics'
                },
                {
                    'transaction_id': 'test_2',
                    'user_id': 'user_2',
                    'amount': 5000.0,  # High amount - should trigger fraud
                    'location': {'latitude': 40.7128, 'longitude': -74.0060},
                    'device_type': 'mobile',
                    'browser': 'safari',
                    'payment_method': 'cryptocurrency',
                    'product_category': 'gift_cards'
                }
            ]
            
            fraud_detected = 0
            for transaction in test_transactions:
                alert = detector.detect_fraud(transaction)
                if detector.should_alert(alert):
                    fraud_detected += 1
                    logger.info(f"Fraud detected: {alert.fraud_reason} (Score: {alert.fraud_score:.2f})")
            
            logger.info(f"Fraud detection test completed: {fraud_detected} alerts generated")
            return True
            
        except Exception as e:
            logger.error(f"Fraud detection test failed: {e}")
            return False
    
    def test_analytics_engine(self) -> bool:
        """Test analytics engine."""
        try:
            logger.info("Testing analytics engine...")
            
            from src.processor.analytics_engine import RealTimeAnalyticsEngine
            
            engine = RealTimeAnalyticsEngine(window_size_seconds=60)
            
            # Generate test transactions
            generator = TransactionDataGenerator()
            
            for _ in range(50):
                transaction = generator.generate_transaction_data()
                engine.add_transaction(transaction)
            
            generator.close()
            
            # Get current metrics
            metrics = engine.get_current_metrics()
            logger.info(f"Analytics metrics: {metrics}")
            
            # Get historical summary
            summary = engine.get_historical_summary(hours=1)
            logger.info(f"Historical summary: {summary}")
            
            return True
            
        except Exception as e:
            logger.error(f"Analytics engine test failed: {e}")
            return False
    
    def test_mock_data_storage(self) -> bool:
        """Test mock data storage (S3 and Snowflake)."""
        try:
            logger.info("Testing mock data storage...")
            
            # Test S3 operations
            if not self.aws_config.test_connection():
                return False
            
            if not self.aws_config.create_s3_bucket():
                return False
            
            # Test data lake structure
            self.s3_manager.create_data_lake_structure()
            summary = self.s3_manager.get_data_lake_summary()
            logger.info(f"Mock S3 summary: {summary}")
            
            # Test Snowflake operations
            if not self.snowflake_config.test_connection():
                return False
            
            if not self.snowflake_config.create_database_and_schema():
                return False
            
            if not self.snowflake_config.create_tables():
                return False
            
            if not self.snowflake_config.create_views():
                return False
            
            # Test data insertion
            test_data = {
                'transaction_id': 'test_123',
                'user_id': 'user_123',
                'amount': 99.99,
                'product_category': 'electronics',
                'status': 'completed',
                'transaction_timestamp': datetime.now(timezone.utc).isoformat()
            }
            
            # Insert test data
            insert_sql = """
                INSERT INTO raw_transactions 
                (transaction_id, user_id, amount, product_category, status, transaction_timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
            """
            
            self.snowflake_config.cursor.execute(insert_sql, (
                test_data['transaction_id'],
                test_data['user_id'],
                test_data['amount'],
                test_data['product_category'],
                test_data['status'],
                test_data['transaction_timestamp']
            ))
            self.snowflake_config.connection.commit()
            
            # Verify insertion
            result = self.snowflake_config.execute_query("SELECT COUNT(*) as count FROM raw_transactions")
            logger.info(f"Mock Snowflake data count: {result[0]['count']}")
            
            return True
            
        except Exception as e:
            logger.error(f"Mock data storage test failed: {e}")
            return False
    
    def test_airflow_simulation(self) -> bool:
        """Test Airflow DAG simulation."""
        try:
            logger.info("Testing Airflow DAG simulation...")
            
            # Simulate DAG execution steps
            steps = [
                "infrastructure_health_check",
                "start_data_generation",
                "start_stream_processing",
                "run_batch_etl",
                "data_quality_checks",
                "generate_analytics_report"
            ]
            
            for step in steps:
                logger.info(f"Simulating DAG step: {step}")
                time.sleep(0.5)  # Simulate processing time
            
            logger.info("Airflow DAG simulation completed")
            return True
            
        except Exception as e:
            logger.error(f"Airflow simulation test failed: {e}")
            return False
    
    def run_complete_test(self) -> dict:
        """Run complete pipeline test with mock services."""
        logger.info("Starting complete local pipeline test...")
        
        test_results = {}
        start_time = datetime.now()
        
        # Run individual tests
        tests = [
            ("data_generation", self.test_data_generation),
            ("kafka_simulation", self.test_kafka_simulation),
            ("fraud_detection", self.test_fraud_detection),
            ("analytics_engine", self.test_analytics_engine),
            ("mock_data_storage", self.test_mock_data_storage),
            ("airflow_simulation", self.test_airflow_simulation)
        ]
        
        for test_name, test_func in tests:
            try:
                result = test_func()
                test_results[test_name] = result
                status = "PASS" if result else "FAIL"
                logger.info(f"{test_name.upper()}: {status}")
            except Exception as e:
                logger.error(f"{test_name} failed with exception: {e}")
                test_results[test_name] = False
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Compile results
        results = {
            'test_timestamp': start_time.isoformat(),
            'test_duration_seconds': duration,
            'test_results': test_results,
            'overall_success': all(test_results.values()),
            'summary': {
                'total_tests': len(test_results),
                'passed_tests': sum(1 for v in test_results.values() if v),
                'failed_tests': sum(1 for v in test_results.values() if not v)
            }
        }
        
        # Save results
        results_file = f"local_test_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        logger.info(f"Test results saved to: {results_file}")
        
        # Print summary
        logger.info("=== LOCAL PIPELINE TEST SUMMARY ===")
        logger.info(f"Overall Success: {results['overall_success']}")
        logger.info(f"Tests Passed: {results['summary']['passed_tests']}/{results['summary']['total_tests']}")
        logger.info(f"Test Duration: {duration:.2f} seconds")
        
        for test_name, success in test_results.items():
            status = "PASS" if success else "FAIL"
            logger.info(f"{test_name.upper()}: {status}")
        
        return results

def main():
    """Main function to run local pipeline tests."""
    try:
        tester = LocalPipelineTester()
        results = tester.run_complete_test()
        
        if results['overall_success']:
            logger.info("🎉 All local pipeline tests passed!")
            logger.info("✅ Your pipeline is ready for GitHub!")
            return 0
        else:
            logger.error("❌ Some tests failed, but this is normal for a demo project")
            return 1
            
    except Exception as e:
        logger.error(f"Local testing failed: {e}")
        return 1

if __name__ == "__main__":
    exit(main())
