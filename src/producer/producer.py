#!/usr/bin/env python3
"""
Streamify Analytics Pipeline - Data Generator
Week 1-2: Foundation & Ingestion

This module generates realistic e-commerce transaction data using the Faker library
and publishes it to a Kafka topic for real-time processing.
"""

import json
import time
import random
import logging
from datetime import datetime, timezone
from typing import Dict, Any
import os
from dotenv import load_dotenv

from faker import Faker
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TransactionDataGenerator:
    """Generates realistic e-commerce transaction data."""
    
    def __init__(self):
        self.fake = Faker()
        self.producer = None
        self.setup_kafka_producer()
        
        # Product categories and their price ranges
        self.product_categories = {
            'Electronics': (50, 2000),
            'Clothing': (10, 500),
            'Books': (5, 50),
            'Home & Garden': (20, 800),
            'Sports': (15, 300),
            'Beauty': (5, 200),
            'Toys': (10, 150),
            'Automotive': (25, 1000)
        }
        
        # Payment methods
        self.payment_methods = ['credit_card', 'debit_card', 'paypal', 'apple_pay', 'google_pay']
        
        # Transaction statuses
        self.transaction_statuses = ['completed', 'pending', 'failed', 'refunded']
        
        # User behavior patterns (some users are more likely to make large purchases)
        self.user_behavior_patterns = {
            'normal': 0.7,      # 70% normal users
            'high_value': 0.2,  # 20% high-value users
            'suspicious': 0.1   # 10% potentially suspicious users
        }
    
    def setup_kafka_producer(self):
        """Initialize Kafka producer."""
        try:
            kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
            self.producer = KafkaProducer(
                bootstrap_servers=[kafka_servers],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                retries=5,
                retry_backoff_ms=100,
                request_timeout_ms=30000
            )
            logger.info(f"Connected to Kafka at {kafka_servers}")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise
    
    def generate_user_behavior_pattern(self) -> str:
        """Generate user behavior pattern based on probabilities."""
        rand = random.random()
        cumulative = 0
        for pattern, prob in self.user_behavior_patterns.items():
            cumulative += prob
            if rand <= cumulative:
                return pattern
        return 'normal'
    
    def generate_transaction_amount(self, behavior_pattern: str) -> float:
        """Generate transaction amount based on user behavior pattern."""
        if behavior_pattern == 'high_value':
            # High-value users: 80% chance of amounts > $500
            if random.random() < 0.8:
                return round(random.uniform(500, 5000), 2)
            else:
                return round(random.uniform(50, 500), 2)
        elif behavior_pattern == 'suspicious':
            # Suspicious users: higher chance of very large or very small amounts
            if random.random() < 0.3:
                return round(random.uniform(2000, 10000), 2)  # Large amounts
            elif random.random() < 0.3:
                return round(random.uniform(0.01, 5), 2)      # Very small amounts
            else:
                return round(random.uniform(10, 200), 2)      # Normal amounts
        else:
            # Normal users: mostly small to medium amounts
            return round(random.uniform(5, 300), 2)
    
    def generate_transaction_data(self) -> Dict[str, Any]:
        """Generate a single transaction record."""
        behavior_pattern = self.generate_user_behavior_pattern()
        
        # Generate user data
        user_id = self.fake.uuid4()
        user_email = self.fake.email()
        user_name = self.fake.name()
        
        # Generate product data
        category = random.choice(list(self.product_categories.keys()))
        min_price, max_price = self.product_categories[category]
        product_id = self.fake.uuid4()
        product_name = f"{self.fake.word().title()} {category[:-1]}"  # Remove 's' from category
        
        # Generate transaction data
        amount = self.generate_transaction_amount(behavior_pattern)
        payment_method = random.choice(self.payment_methods)
        status = random.choices(
            self.transaction_statuses,
            weights=[0.85, 0.10, 0.03, 0.02]  # Most transactions are completed
        )[0]
        
        # Generate location data
        location = {
            'country': self.fake.country_code(),
            'state': self.fake.state(),
            'city': self.fake.city(),
            'zip_code': self.fake.zipcode(),
            'latitude': float(self.fake.latitude()),
            'longitude': float(self.fake.longitude())
        }
        
        # Generate timestamp
        timestamp = datetime.now(timezone.utc).isoformat()
        
        # Create transaction record
        transaction = {
            'transaction_id': self.fake.uuid4(),
            'user_id': user_id,
            'user_email': user_email,
            'user_name': user_name,
            'product_id': product_id,
            'product_name': product_name,
            'product_category': category,
            'amount': amount,
            'currency': 'USD',
            'payment_method': payment_method,
            'status': status,
            'timestamp': timestamp,
            'location': location,
            'behavior_pattern': behavior_pattern,
            'session_id': self.fake.uuid4(),
            'device_type': random.choice(['mobile', 'desktop', 'tablet']),
            'browser': random.choice(['chrome', 'firefox', 'safari', 'edge']),
            'ip_address': self.fake.ipv4()
        }
        
        return transaction
    
    def send_transaction(self, transaction: Dict[str, Any]) -> bool:
        """Send transaction to Kafka topic."""
        try:
            topic = os.getenv('KAFKA_TOPIC_SALES', 'sales_stream')
            key = transaction['transaction_id']
            
            future = self.producer.send(topic, value=transaction, key=key)
            record_metadata = future.get(timeout=10)
            
            logger.debug(
                f"Transaction sent to topic {record_metadata.topic} "
                f"partition {record_metadata.partition} "
                f"offset {record_metadata.offset}"
            )
            return True
            
        except KafkaError as e:
            logger.error(f"Failed to send transaction: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending transaction: {e}")
            return False
    
    def generate_and_send_batch(self, batch_size: int = 10) -> int:
        """Generate and send a batch of transactions."""
        successful_sends = 0
        
        for _ in range(batch_size):
            transaction = self.generate_transaction_data()
            if self.send_transaction(transaction):
                successful_sends += 1
                logger.info(
                    f"Generated transaction: {transaction['transaction_id']} "
                    f"Amount: ${transaction['amount']} "
                    f"Category: {transaction['product_category']} "
                    f"Pattern: {transaction['behavior_pattern']}"
                )
        
        return successful_sends
    
    def run_continuous_generation(self, 
                                batch_size: int = 10, 
                                batch_interval: float = 1.0,
                                duration_minutes: int = None):
        """Run continuous data generation."""
        logger.info(f"Starting continuous data generation...")
        logger.info(f"Batch size: {batch_size}, Interval: {batch_interval}s")
        
        start_time = time.time()
        total_transactions = 0
        
        try:
            while True:
                batch_start = time.time()
                
                # Generate and send batch
                successful = self.generate_and_send_batch(batch_size)
                total_transactions += successful
                
                # Log batch statistics
                batch_duration = time.time() - batch_start
                logger.info(
                    f"Batch completed: {successful}/{batch_size} transactions sent "
                    f"in {batch_duration:.2f}s. Total: {total_transactions}"
                )
                
                # Check if we should stop
                if duration_minutes and (time.time() - start_time) > (duration_minutes * 60):
                    logger.info(f"Reached duration limit of {duration_minutes} minutes")
                    break
                
                # Wait before next batch
                time.sleep(batch_interval)
                
        except KeyboardInterrupt:
            logger.info("Data generation stopped by user")
        except Exception as e:
            logger.error(f"Error during data generation: {e}")
        finally:
            self.close()
            logger.info(f"Data generation completed. Total transactions: {total_transactions}")
    
    def close(self):
        """Close the Kafka producer."""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka producer closed")


def main():
    """Main function to run the data generator."""
    generator = TransactionDataGenerator()
    
    # Configuration
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', '10'))
    BATCH_INTERVAL = float(os.getenv('BATCH_INTERVAL', '1.0'))
    DURATION_MINUTES = int(os.getenv('DURATION_MINUTES', '0'))  # 0 = run indefinitely
    
    try:
        if DURATION_MINUTES > 0:
            generator.run_continuous_generation(
                batch_size=BATCH_SIZE,
                batch_interval=BATCH_INTERVAL,
                duration_minutes=DURATION_MINUTES
            )
        else:
            generator.run_continuous_generation(
                batch_size=BATCH_SIZE,
                batch_interval=BATCH_INTERVAL
            )
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
