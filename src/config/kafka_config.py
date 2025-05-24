"""
Kafka Configuration Module
Week 2: Kafka and Zookeeper setup
"""

import os
from typing import Dict, Any

class KafkaConfig:
    """Configuration class for Kafka settings."""
    
    def __init__(self):
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.sales_topic = os.getenv('KAFKA_TOPIC_SALES', 'sales_stream')
        self.fraud_topic = os.getenv('KAFKA_TOPIC_FRAUD', 'fraud_alerts')
        self.analytics_topic = os.getenv('KAFKA_TOPIC_ANALYTICS', 'analytics_stream')
        
        # Producer configuration
        self.producer_config = {
            'bootstrap_servers': [self.bootstrap_servers],
            'value_serializer': lambda v: v.encode('utf-8') if isinstance(v, str) else v,
            'key_serializer': lambda k: k.encode('utf-8') if k else None,
            'retries': 5,
            'retry_backoff_ms': 100,
            'request_timeout_ms': 30000,
            'acks': 'all',  # Wait for all replicas to acknowledge
            'enable_idempotence': True,  # Prevent duplicate messages
            'compression_type': 'gzip'
        }
        
        # Consumer configuration
        self.consumer_config = {
            'bootstrap_servers': [self.bootstrap_servers],
            'group_id': 'streamify_analytics_group',
            'auto_offset_reset': 'latest',
            'enable_auto_commit': True,
            'auto_commit_interval_ms': 1000,
            'session_timeout_ms': 30000,
            'heartbeat_interval_ms': 10000,
            'max_poll_records': 100,
            'value_deserializer': lambda m: m.decode('utf-8') if m else None,
            'key_deserializer': lambda m: m.decode('utf-8') if m else None
        }
    
    def get_topic_config(self, topic_name: str) -> Dict[str, Any]:
        """Get topic-specific configuration."""
        return {
            'num_partitions': 3,
            'replication_factor': 1,
            'config': {
                'cleanup.policy': 'delete',
                'retention.ms': 604800000,  # 7 days
                'segment.ms': 86400000,     # 1 day
                'compression.type': 'gzip'
            }
        }
    
    def get_spark_kafka_config(self) -> Dict[str, str]:
        """Get Kafka configuration for Spark Streaming."""
        return {
            'kafka.bootstrap.servers': self.bootstrap_servers,
            'subscribe': self.sales_topic,
            'startingOffsets': 'latest',
            'failOnDataLoss': 'false',
            'kafka.security.protocol': 'PLAINTEXT'
        }
