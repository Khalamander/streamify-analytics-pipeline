#!/usr/bin/env python3
"""
Kafka Topic Setup Script
Week 2: Kafka and Zookeeper setup

This script creates the necessary Kafka topics for the Streamify Analytics Pipeline.
"""

import sys
import time
import logging
from kafka.admin import KafkaAdminClient, ConfigResource, ConfigResourceType
from kafka.admin.new_topic import NewTopic
from kafka.errors import TopicAlreadyExistsError, KafkaError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_kafka_topics():
    """Create Kafka topics for the analytics pipeline."""
    
    # Kafka configuration
    bootstrap_servers = ['localhost:9092']
    
    # Topic configurations
    topics = [
        {
            'name': 'sales_stream',
            'num_partitions': 3,
            'replication_factor': 1,
            'config': {
                'cleanup.policy': 'delete',
                'retention.ms': '604800000',  # 7 days
                'segment.ms': '86400000',     # 1 day
                'compression.type': 'gzip'
            }
        },
        {
            'name': 'fraud_alerts',
            'num_partitions': 2,
            'replication_factor': 1,
            'config': {
                'cleanup.policy': 'delete',
                'retention.ms': '2592000000',  # 30 days
                'segment.ms': '86400000',      # 1 day
                'compression.type': 'gzip'
            }
        },
        {
            'name': 'analytics_stream',
            'num_partitions': 2,
            'replication_factor': 1,
            'config': {
                'cleanup.policy': 'delete',
                'retention.ms': '604800000',  # 7 days
                'segment.ms': '86400000',     # 1 day
                'compression.type': 'gzip'
            }
        }
    ]
    
    try:
        # Create admin client
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='streamify_admin'
        )
        
        logger.info("Connected to Kafka cluster")
        
        # Create topics
        new_topics = []
        for topic_config in topics:
            topic = NewTopic(
                name=topic_config['name'],
                num_partitions=topic_config['num_partitions'],
                replication_factor=topic_config['replication_factor'],
                topic_configs=topic_config['config']
            )
            new_topics.append(topic)
        
        # Create topics
        try:
            fs = admin_client.create_topics(new_topics, validate_only=False)
            for topic, f in fs.items():
                try:
                    f.result()  # The result itself is None
                    logger.info(f"Topic '{topic}' created successfully")
                except TopicAlreadyExistsError:
                    logger.warning(f"Topic '{topic}' already exists")
                except Exception as e:
                    logger.error(f"Failed to create topic '{topic}': {e}")
        except Exception as e:
            logger.error(f"Error creating topics: {e}")
            return False
        
        # Wait a moment for topics to be created
        time.sleep(2)
        
        # Verify topics were created
        metadata = admin_client.describe_topics()
        created_topics = list(metadata.keys())
        
        logger.info(f"Available topics: {created_topics}")
        
        # Verify our topics exist
        expected_topics = [topic['name'] for topic in topics]
        missing_topics = set(expected_topics) - set(created_topics)
        
        if missing_topics:
            logger.error(f"Missing topics: {missing_topics}")
            return False
        
        logger.info("All topics created successfully!")
        return True
        
    except KafkaError as e:
        logger.error(f"Kafka error: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False
    finally:
        try:
            admin_client.close()
        except:
            pass

def main():
    """Main function."""
    logger.info("Setting up Kafka topics for Streamify Analytics Pipeline...")
    
    success = create_kafka_topics()
    
    if success:
        logger.info("Kafka topics setup completed successfully!")
        return 0
    else:
        logger.error("Kafka topics setup failed!")
        return 1

if __name__ == "__main__":
    sys.exit(main())
