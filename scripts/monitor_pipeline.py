#!/usr/bin/env python3
"""
Pipeline Monitoring Script
Week 11: Complete Orchestration Pipeline and Testing

This script provides real-time monitoring of the analytics pipeline.
"""

import sys
import os
import time
import logging
import json
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List
import psutil

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config.kafka_config import KafkaConfig
from src.config.aws_config import AWSConfig
from src.config.snowflake_config import SnowflakeConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PipelineMonitor:
    """Real-time pipeline monitoring and alerting."""
    
    def __init__(self):
        self.kafka_config = KafkaConfig()
        self.aws_config = AWSConfig()
        self.snowflake_config = SnowflakeConfig()
        self.alert_thresholds = {
            'kafka_lag': 1000,
            's3_storage_usage': 0.8,  # 80% of bucket capacity
            'snowflake_query_timeout': 30,  # seconds
            'memory_usage': 0.9,  # 90% of available memory
            'cpu_usage': 0.8  # 80% CPU usage
        }
        self.alerts = []
    
    def check_kafka_health(self) -> Dict[str, Any]:
        """Check Kafka cluster health and performance."""
        try:
            from kafka import KafkaConsumer
            from kafka.admin import KafkaAdminClient
            
            # Check cluster metadata
            admin_client = KafkaAdminClient(
                bootstrap_servers=[self.kafka_config.bootstrap_servers]
            )
            
            metadata = admin_client.describe_cluster()
            cluster_id = metadata.cluster_id
            controller_id = metadata.controller_id
            brokers = metadata.brokers
            
            # Check topic metadata
            topics = admin_client.list_topics()
            
            # Check consumer lag
            consumer = KafkaConsumer(
                self.kafka_config.sales_topic,
                bootstrap_servers=[self.kafka_config.bootstrap_servers],
                group_id='monitoring_group'
            )
            
            # Get partition information
            partitions = consumer.partitions_for_topic(self.kafka_config.sales_topic)
            
            kafka_health = {
                'cluster_id': cluster_id,
                'controller_id': controller_id,
                'broker_count': len(brokers),
                'topic_count': len(topics),
                'partitions': len(partitions) if partitions else 0,
                'status': 'healthy'
            }
            
            consumer.close()
            admin_client.close()
            
            return kafka_health
            
        except Exception as e:
            logger.error(f"Kafka health check failed: {e}")
            return {'status': 'unhealthy', 'error': str(e)}
    
    def check_s3_health(self) -> Dict[str, Any]:
        """Check S3 data lake health and storage usage."""
        try:
            # Get bucket information
            bucket_name = self.aws_config.s3_bucket
            
            # List objects to calculate storage usage
            objects = self.aws_config.list_s3_objects('')
            
            total_size = sum(obj['Size'] for obj in objects)
            object_count = len(objects)
            
            # Get bucket size (approximate)
            bucket_size_gb = total_size / (1024 ** 3)
            
            # Check for recent data
            recent_objects = [
                obj for obj in objects 
                if obj['LastModified'] > datetime.now(timezone.utc) - timedelta(hours=1)
            ]
            
            s3_health = {
                'bucket_name': bucket_name,
                'total_objects': object_count,
                'total_size_gb': round(bucket_size_gb, 2),
                'recent_objects_1h': len(recent_objects),
                'status': 'healthy'
            }
            
            # Check storage threshold
            if bucket_size_gb > 100:  # Assuming 100GB threshold
                s3_health['storage_warning'] = True
                s3_health['status'] = 'warning'
            
            return s3_health
            
        except Exception as e:
            logger.error(f"S3 health check failed: {e}")
            return {'status': 'unhealthy', 'error': str(e)}
    
    def check_snowflake_health(self) -> Dict[str, Any]:
        """Check Snowflake data warehouse health."""
        try:
            if not self.snowflake_config.connect():
                return {'status': 'unhealthy', 'error': 'Connection failed'}
            
            # Check query performance
            start_time = time.time()
            result = self.snowflake_config.execute_query("SELECT CURRENT_TIMESTAMP()")
            query_time = time.time() - start_time
            
            # Get table statistics
            table_stats = self.snowflake_config.get_database_summary()
            
            # Check for recent data
            recent_data = self.snowflake_config.execute_query("""
                SELECT COUNT(*) as count 
                FROM raw_transactions 
                WHERE created_at >= CURRENT_TIMESTAMP() - INTERVAL '1 hour'
            """)
            
            snowflake_health = {
                'query_response_time': round(query_time, 2),
                'table_stats': table_stats,
                'recent_transactions_1h': recent_data[0]['COUNT'] if recent_data else 0,
                'status': 'healthy'
            }
            
            # Check query performance threshold
            if query_time > self.alert_thresholds['snowflake_query_timeout']:
                snowflake_health['performance_warning'] = True
                snowflake_health['status'] = 'warning'
            
            self.snowflake_config.disconnect()
            return snowflake_health
            
        except Exception as e:
            logger.error(f"Snowflake health check failed: {e}")
            return {'status': 'unhealthy', 'error': str(e)}
    
    def check_system_resources(self) -> Dict[str, Any]:
        """Check system resource usage."""
        try:
            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)
            
            # Memory usage
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            
            # Disk usage
            disk = psutil.disk_usage('/')
            disk_percent = disk.percent
            
            # Network I/O
            network = psutil.net_io_counters()
            
            system_health = {
                'cpu_percent': cpu_percent,
                'memory_percent': memory_percent,
                'memory_available_gb': round(memory.available / (1024 ** 3), 2),
                'disk_percent': disk_percent,
                'disk_free_gb': round(disk.free / (1024 ** 3), 2),
                'network_bytes_sent': network.bytes_sent,
                'network_bytes_recv': network.bytes_recv,
                'status': 'healthy'
            }
            
            # Check resource thresholds
            if cpu_percent > self.alert_thresholds['cpu_usage'] * 100:
                system_health['cpu_warning'] = True
                system_health['status'] = 'warning'
            
            if memory_percent > self.alert_thresholds['memory_usage'] * 100:
                system_health['memory_warning'] = True
                system_health['status'] = 'warning'
            
            return system_health
            
        except Exception as e:
            logger.error(f"System resource check failed: {e}")
            return {'status': 'unhealthy', 'error': str(e)}
    
    def check_pipeline_throughput(self) -> Dict[str, Any]:
        """Check pipeline data throughput metrics."""
        try:
            if not self.snowflake_config.connect():
                return {'status': 'unhealthy', 'error': 'Snowflake connection failed'}
            
            # Get throughput metrics for last hour
            throughput_metrics = self.snowflake_config.execute_query("""
                SELECT 
                    COUNT(*) as total_transactions,
                    COUNT(DISTINCT user_id) as unique_users,
                    SUM(amount) as total_revenue,
                    AVG(amount) as avg_transaction_amount,
                    COUNT(CASE WHEN created_at >= CURRENT_TIMESTAMP() - INTERVAL '10 minutes' THEN 1 END) as recent_transactions
                FROM raw_transactions
                WHERE created_at >= CURRENT_TIMESTAMP() - INTERVAL '1 hour'
            """)
            
            # Get fraud detection metrics
            fraud_metrics = self.snowflake_config.execute_query("""
                SELECT 
                    COUNT(*) as total_alerts,
                    COUNT(CASE WHEN alert_level = 'CRITICAL' THEN 1 END) as critical_alerts,
                    AVG(fraud_score) as avg_fraud_score
                FROM fraud_alerts
                WHERE alert_timestamp >= CURRENT_TIMESTAMP() - INTERVAL '1 hour'
            """)
            
            throughput = {
                'transactions_per_hour': throughput_metrics[0]['TOTAL_TRANSACTIONS'] if throughput_metrics else 0,
                'unique_users_per_hour': throughput_metrics[0]['UNIQUE_USERS'] if throughput_metrics else 0,
                'revenue_per_hour': float(throughput_metrics[0]['TOTAL_REVENUE']) if throughput_metrics else 0,
                'avg_transaction_amount': float(throughput_metrics[0]['AVG_TRANSACTION_AMOUNT']) if throughput_metrics else 0,
                'recent_transactions_10min': throughput_metrics[0]['RECENT_TRANSACTIONS'] if throughput_metrics else 0,
                'fraud_alerts_per_hour': fraud_metrics[0]['TOTAL_ALERTS'] if fraud_metrics else 0,
                'critical_alerts_per_hour': fraud_metrics[0]['CRITICAL_ALERTS'] if fraud_metrics else 0,
                'avg_fraud_score': float(fraud_metrics[0]['AVG_FRAUD_SCORE']) if fraud_metrics else 0,
                'status': 'healthy'
            }
            
            # Check for low throughput
            if throughput['recent_transactions_10min'] < 10:
                throughput['low_throughput_warning'] = True
                throughput['status'] = 'warning'
            
            self.snowflake_config.disconnect()
            return throughput
            
        except Exception as e:
            logger.error(f"Pipeline throughput check failed: {e}")
            return {'status': 'unhealthy', 'error': str(e)}
    
    def generate_alerts(self, health_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate alerts based on health data."""
        alerts = []
        current_time = datetime.now(timezone.utc)
        
        # Check each component for issues
        for component, data in health_data.items():
            if data.get('status') == 'unhealthy':
                alert = {
                    'timestamp': current_time.isoformat(),
                    'component': component,
                    'level': 'CRITICAL',
                    'message': f"{component.upper()} is unhealthy: {data.get('error', 'Unknown error')}",
                    'action_required': True
                }
                alerts.append(alert)
            
            elif data.get('status') == 'warning':
                alert = {
                    'timestamp': current_time.isoformat(),
                    'component': component,
                    'level': 'WARNING',
                    'message': f"{component.upper()} is showing warning signs",
                    'action_required': False
                }
                alerts.append(alert)
        
        return alerts
    
    def run_monitoring_cycle(self) -> Dict[str, Any]:
        """Run a complete monitoring cycle."""
        logger.info("Starting monitoring cycle...")
        
        cycle_start = datetime.now(timezone.utc)
        
        # Collect health data from all components
        health_data = {
            'kafka': self.check_kafka_health(),
            's3': self.check_s3_health(),
            'snowflake': self.check_snowflake_health(),
            'system': self.check_system_resources(),
            'throughput': self.check_pipeline_throughput()
        }
        
        # Generate alerts
        alerts = self.generate_alerts(health_data)
        
        # Create monitoring report
        report = {
            'timestamp': cycle_start.isoformat(),
            'cycle_duration_seconds': (datetime.now(timezone.utc) - cycle_start).total_seconds(),
            'health_data': health_data,
            'alerts': alerts,
            'overall_status': 'healthy' if not alerts else 'warning' if any(a['level'] == 'WARNING' for a in alerts) else 'critical'
        }
        
        # Log alerts
        for alert in alerts:
            if alert['level'] == 'CRITICAL':
                logger.error(f"CRITICAL ALERT: {alert['message']}")
            elif alert['level'] == 'WARNING':
                logger.warning(f"WARNING: {alert['message']}")
        
        return report
    
    def start_monitoring(self, interval_seconds: int = 60, duration_minutes: int = None):
        """Start continuous monitoring."""
        logger.info(f"Starting pipeline monitoring (interval: {interval_seconds}s)")
        
        start_time = datetime.now()
        cycle_count = 0
        
        try:
            while True:
                cycle_count += 1
                logger.info(f"Monitoring cycle #{cycle_count}")
                
                # Run monitoring cycle
                report = self.run_monitoring_cycle()
                
                # Save report
                report_file = f"/tmp/pipeline_monitor_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                with open(report_file, 'w') as f:
                    json.dump(report, f, indent=2, default=str)
                
                # Check if we should stop
                if duration_minutes and (datetime.now() - start_time).total_seconds() > duration_minutes * 60:
                    logger.info(f"Monitoring completed after {duration_minutes} minutes")
                    break
                
                # Wait for next cycle
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            logger.info("Monitoring stopped by user")
        except Exception as e:
            logger.error(f"Monitoring error: {e}")
            raise

def main():
    """Main function to run pipeline monitoring."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Monitor Streamify Analytics Pipeline')
    parser.add_argument('--interval', type=int, default=60,
                       help='Monitoring interval in seconds (default: 60)')
    parser.add_argument('--duration', type=int, default=None,
                       help='Monitoring duration in minutes (default: run indefinitely)')
    
    args = parser.parse_args()
    
    try:
        monitor = PipelineMonitor()
        monitor.start_monitoring(
            interval_seconds=args.interval,
            duration_minutes=args.duration
        )
    except KeyboardInterrupt:
        logger.info("Monitoring stopped by user")
    except Exception as e:
        logger.error(f"Monitoring failed: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
