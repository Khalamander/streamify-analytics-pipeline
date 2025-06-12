"""
Streamify Analytics Pipeline - Airflow DAG
Week 9-11: Batch Orchestration with Airflow

This DAG orchestrates the complete data pipeline from data generation
to data warehouse loading with comprehensive monitoring and error handling.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable
from airflow.utils.dates import days_ago
import logging

# Default arguments
default_args = {
    'owner': 'streamify_analytics_team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['admin@streamify.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# DAG definition
dag = DAG(
    'streamify_analytics_pipeline',
    default_args=default_args,
    description='End-to-end analytics pipeline for real-time e-commerce data',
    schedule_interval='@hourly',  # Run every hour
    max_active_runs=1,
    tags=['analytics', 'e-commerce', 'real-time', 'fraud-detection']
)

def check_infrastructure_health():
    """Check health of all infrastructure components."""
    import subprocess
    import sys
    
    logger = logging.getLogger(__name__)
    
    # Check Kafka
    try:
        result = subprocess.run(['kafka-topics', '--bootstrap-server', 'localhost:9092', '--list'], 
                              capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            logger.info("Kafka is healthy")
        else:
            raise Exception(f"Kafka health check failed: {result.stderr}")
    except Exception as e:
        logger.error(f"Kafka health check error: {e}")
        raise
    
    # Check Spark
    try:
        result = subprocess.run(['spark-submit', '--version'], 
                              capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            logger.info("Spark is healthy")
        else:
            raise Exception(f"Spark health check failed: {result.stderr}")
    except Exception as e:
        logger.error(f"Spark health check error: {e}")
        raise
    
    # Check AWS connectivity
    try:
        import boto3
        s3 = boto3.client('s3')
        s3.list_buckets()
        logger.info("AWS S3 is accessible")
    except Exception as e:
        logger.error(f"AWS S3 health check error: {e}")
        raise
    
    # Check Snowflake connectivity
    try:
        import snowflake.connector
        conn = snowflake.connector.connect(
            user=Variable.get('SNOWFLAKE_USER'),
            password=Variable.get('SNOWFLAKE_PASSWORD'),
            account=Variable.get('SNOWFLAKE_ACCOUNT'),
            warehouse=Variable.get('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
            database=Variable.get('SNOWFLAKE_DATABASE', 'STREAMIFY_ANALYTICS'),
            schema=Variable.get('SNOWFLAKE_SCHEMA', 'PUBLIC')
        )
        conn.close()
        logger.info("Snowflake is accessible")
    except Exception as e:
        logger.error(f"Snowflake health check error: {e}")
        raise
    
    logger.info("All infrastructure components are healthy")

def start_data_generation():
    """Start the data generation process."""
    import subprocess
    import sys
    import os
    
    logger = logging.getLogger(__name__)
    
    # Set environment variables
    env = os.environ.copy()
    env.update({
        'BATCH_SIZE': '50',
        'BATCH_INTERVAL': '2.0',
        'DURATION_MINUTES': '60'  # Run for 1 hour
    })
    
    try:
        # Start data generation in background
        process = subprocess.Popen([
            sys.executable, 
            '/opt/airflow/dags/scripts/start_data_generation.py'
        ], env=env)
        
        # Store process ID for monitoring
        with open('/tmp/data_generation.pid', 'w') as f:
            f.write(str(process.pid))
        
        logger.info(f"Data generation started with PID: {process.pid}")
        
    except Exception as e:
        logger.error(f"Failed to start data generation: {e}")
        raise

def start_stream_processing():
    """Start the stream processing pipeline."""
    import subprocess
    import sys
    import os
    
    logger = logging.getLogger(__name__)
    
    try:
        # Start stream processing
        process = subprocess.Popen([
            sys.executable, 
            '/opt/airflow/dags/src/processor/stream_processor.py'
        ])
        
        # Store process ID for monitoring
        with open('/tmp/stream_processing.pid', 'w') as f:
            f.write(str(process.pid))
        
        logger.info(f"Stream processing started with PID: {process.pid}")
        
    except Exception as e:
        logger.error(f"Failed to start stream processing: {e}")
        raise

def run_batch_etl():
    """Run batch ETL process to load data from S3 to Snowflake."""
    import sys
    import os
    
    # Add project root to Python path
    sys.path.append('/opt/airflow/dags')
    
    from src.processor.data_warehouse_loader import DataWarehouseLoader
    
    logger = logging.getLogger(__name__)
    
    try:
        loader = DataWarehouseLoader()
        
        # Run full data load
        success = loader.run_full_data_load()
        
        if success:
            logger.info("Batch ETL completed successfully")
        else:
            raise Exception("Batch ETL failed")
        
        # Get loading statistics
        stats = loader.get_loading_statistics()
        logger.info(f"Loading statistics: {stats}")
        
        loader.cleanup()
        
    except Exception as e:
        logger.error(f"Batch ETL error: {e}")
        raise

def run_data_quality_checks():
    """Run data quality checks on loaded data."""
    import sys
    import os
    
    # Add project root to Python path
    sys.path.append('/opt/airflow/dags')
    
    from src.config.snowflake_config import SnowflakeConfig
    
    logger = logging.getLogger(__name__)
    
    try:
        snowflake = SnowflakeConfig()
        if not snowflake.connect():
            raise Exception("Failed to connect to Snowflake")
        
        # Data quality checks
        checks = []
        
        # Check for null transaction IDs
        null_id_check = snowflake.execute_query("""
            SELECT COUNT(*) as null_count 
            FROM raw_transactions 
            WHERE transaction_id IS NULL
        """)
        checks.append(('null_transaction_ids', null_id_check[0]['NULL_COUNT']))
        
        # Check for negative amounts
        negative_amount_check = snowflake.execute_query("""
            SELECT COUNT(*) as negative_count 
            FROM raw_transactions 
            WHERE amount < 0
        """)
        checks.append(('negative_amounts', negative_amount_check[0]['NEGATIVE_COUNT']))
        
        # Check for duplicate transactions
        duplicate_check = snowflake.execute_query("""
            SELECT COUNT(*) as duplicate_count 
            FROM (
                SELECT transaction_id, COUNT(*) as cnt 
                FROM raw_transactions 
                GROUP BY transaction_id 
                HAVING COUNT(*) > 1
            )
        """)
        checks.append(('duplicate_transactions', duplicate_check[0]['DUPLICATE_COUNT']))
        
        # Check data freshness
        freshness_check = snowflake.execute_query("""
            SELECT MAX(transaction_timestamp) as latest_transaction 
            FROM raw_transactions
        """)
        latest_transaction = freshness_check[0]['LATEST_TRANSACTION']
        
        # Log results
        for check_name, count in checks:
            if count > 0:
                logger.warning(f"Data quality issue: {check_name} = {count}")
            else:
                logger.info(f"Data quality check passed: {check_name}")
        
        logger.info(f"Latest transaction: {latest_transaction}")
        
        snowflake.disconnect()
        
    except Exception as e:
        logger.error(f"Data quality check error: {e}")
        raise

def generate_analytics_report():
    """Generate analytics report from data warehouse."""
    import sys
    import os
    import json
    from datetime import datetime, timezone
    
    # Add project root to Python path
    sys.path.append('/opt/airflow/dags')
    
    from src.config.snowflake_config import SnowflakeConfig
    
    logger = logging.getLogger(__name__)
    
    try:
        snowflake = SnowflakeConfig()
        if not snowflake.connect():
            raise Exception("Failed to connect to Snowflake")
        
        # Generate analytics report
        report = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'summary': {},
            'daily_metrics': {},
            'fraud_analysis': {},
            'product_performance': {}
        }
        
        # Get daily summary
        daily_summary = snowflake.execute_query("""
            SELECT * FROM daily_sales_summary 
            ORDER BY sale_date DESC 
            LIMIT 7
        """)
        report['daily_metrics'] = daily_summary
        
        # Get fraud analysis
        fraud_analysis = snowflake.execute_query("""
            SELECT * FROM fraud_analysis 
            ORDER BY alert_date DESC 
            LIMIT 7
        """)
        report['fraud_analysis'] = fraud_analysis
        
        # Get product performance
        product_performance = snowflake.execute_query("""
            SELECT * FROM product_performance 
            ORDER BY total_revenue DESC 
            LIMIT 10
        """)
        report['product_performance'] = product_performance
        
        # Get overall summary
        summary_query = snowflake.execute_query("""
            SELECT 
                COUNT(*) as total_transactions,
                SUM(amount) as total_revenue,
                AVG(amount) as avg_transaction_value,
                COUNT(DISTINCT user_id) as unique_customers
            FROM raw_transactions
            WHERE DATE(transaction_timestamp) = CURRENT_DATE()
        """)
        report['summary'] = summary_query[0] if summary_query else {}
        
        # Save report
        report_path = f"/opt/airflow/logs/analytics_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        logger.info(f"Analytics report generated: {report_path}")
        
        snowflake.disconnect()
        
    except Exception as e:
        logger.error(f"Analytics report generation error: {e}")
        raise

def cleanup_old_data():
    """Clean up old data to manage storage costs."""
    import sys
    import os
    
    # Add project root to Python path
    sys.path.append('/opt/airflow/dags')
    
    from src.config.aws_config import AWSConfig
    from src.config.snowflake_config import SnowflakeConfig
    
    logger = logging.getLogger(__name__)
    
    try:
        # Clean up old S3 data (older than 30 days)
        aws_config = AWSConfig()
        
        # Get old objects
        cutoff_date = datetime.now() - timedelta(days=30)
        old_objects = aws_config.list_s3_objects('raw_transactions/')
        
        deleted_count = 0
        for obj in old_objects:
            if obj['LastModified'].replace(tzinfo=None) < cutoff_date:
                if aws_config.delete_s3_object(obj['Key']):
                    deleted_count += 1
        
        logger.info(f"Deleted {deleted_count} old S3 objects")
        
        # Clean up old Snowflake data (older than 90 days)
        snowflake = SnowflakeConfig()
        if snowflake.connect():
            # Delete old raw transactions
            snowflake.execute_query("""
                DELETE FROM raw_transactions 
                WHERE transaction_timestamp < DATEADD(day, -90, CURRENT_TIMESTAMP())
            """)
            
            # Delete old fraud alerts
            snowflake.execute_query("""
                DELETE FROM fraud_alerts 
                WHERE alert_timestamp < DATEADD(day, -90, CURRENT_TIMESTAMP())
            """)
            
            snowflake.disconnect()
            logger.info("Cleaned up old Snowflake data")
        
    except Exception as e:
        logger.error(f"Data cleanup error: {e}")
        raise

# Define tasks
start_pipeline = DummyOperator(
    task_id='start_pipeline',
    dag=dag
)

infrastructure_health_check = PythonOperator(
    task_id='infrastructure_health_check',
    python_callable=check_infrastructure_health,
    dag=dag
)

start_data_generation_task = PythonOperator(
    task_id='start_data_generation',
    python_callable=start_data_generation,
    dag=dag
)

start_stream_processing_task = PythonOperator(
    task_id='start_stream_processing',
    python_callable=start_stream_processing,
    dag=dag
)

wait_for_data = BashOperator(
    task_id='wait_for_data',
    bash_command='sleep 300',  # Wait 5 minutes for data to accumulate
    dag=dag
)

run_batch_etl_task = PythonOperator(
    task_id='run_batch_etl',
    python_callable=run_batch_etl,
    dag=dag
)

data_quality_checks = PythonOperator(
    task_id='data_quality_checks',
    python_callable=run_data_quality_checks,
    dag=dag
)

generate_analytics_report_task = PythonOperator(
    task_id='generate_analytics_report',
    python_callable=generate_analytics_report,
    dag=dag
)

cleanup_old_data_task = PythonOperator(
    task_id='cleanup_old_data',
    python_callable=cleanup_old_data,
    dag=dag
)

end_pipeline = DummyOperator(
    task_id='end_pipeline',
    dag=dag
)

# Define task dependencies
start_pipeline >> infrastructure_health_check

infrastructure_health_check >> [start_data_generation_task, start_stream_processing_task]

start_data_generation_task >> wait_for_data
start_stream_processing_task >> wait_for_data

wait_for_data >> run_batch_etl_task

run_batch_etl_task >> data_quality_checks

data_quality_checks >> generate_analytics_report_task

generate_analytics_report_task >> cleanup_old_data_task

cleanup_old_data_task >> end_pipeline
