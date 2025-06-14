"""
Fraud Detection Monitoring DAG
Week 9-11: Batch Orchestration with Airflow

This DAG monitors fraud detection performance and generates alerts
for high-risk patterns and system issues.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
import logging

# Default arguments
default_args = {
    'owner': 'fraud_detection_team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['fraud-team@streamify.com', 'security@streamify.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'catchup': False
}

# DAG definition
dag = DAG(
    'fraud_detection_monitoring',
    default_args=default_args,
    description='Monitor fraud detection performance and generate alerts',
    schedule_interval='*/15 * * * *',  # Run every 15 minutes
    max_active_runs=1,
    tags=['fraud', 'monitoring', 'security', 'alerts']
)

def check_fraud_detection_health():
    """Check the health of fraud detection system."""
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
        
        # Check recent fraud detection activity
        recent_alerts = snowflake.execute_query("""
            SELECT 
                COUNT(*) as total_alerts,
                COUNT(CASE WHEN alert_level = 'CRITICAL' THEN 1 END) as critical_alerts,
                COUNT(CASE WHEN alert_level = 'HIGH' THEN 1 END) as high_alerts,
                AVG(fraud_score) as avg_fraud_score,
                MAX(alert_timestamp) as latest_alert
            FROM fraud_alerts
            WHERE alert_timestamp >= CURRENT_TIMESTAMP() - INTERVAL '15 minutes'
        """)
        
        if recent_alerts:
            stats = recent_alerts[0]
            logger.info(f"Fraud detection stats: {stats}")
            
            # Check for unusual activity
            if stats['CRITICAL_ALERTS'] > 10:
                raise Exception(f"High number of critical alerts: {stats['CRITICAL_ALERTS']}")
            
            if stats['AVG_FRAUD_SCORE'] > 0.8:
                raise Exception(f"High average fraud score: {stats['AVG_FRAUD_SCORE']}")
        
        snowflake.disconnect()
        
    except Exception as e:
        logger.error(f"Fraud detection health check error: {e}")
        raise

def analyze_fraud_patterns():
    """Analyze fraud patterns and trends."""
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
        
        # Analyze fraud patterns
        patterns = snowflake.execute_query("""
            SELECT 
                DATE(alert_timestamp) as alert_date,
                alert_level,
                COUNT(*) as alert_count,
                AVG(fraud_score) as avg_score,
                COUNT(DISTINCT user_id) as affected_users
            FROM fraud_alerts
            WHERE alert_timestamp >= CURRENT_TIMESTAMP() - INTERVAL '24 hours'
            GROUP BY DATE(alert_timestamp), alert_level
            ORDER BY alert_date DESC, alert_level
        """)
        
        # Analyze fraud reasons
        fraud_reasons = snowflake.execute_query("""
            SELECT 
                fraud_reason,
                COUNT(*) as frequency,
                AVG(fraud_score) as avg_score
            FROM fraud_alerts
            WHERE alert_timestamp >= CURRENT_TIMESTAMP() - INTERVAL '24 hours'
            GROUP BY fraud_reason
            ORDER BY frequency DESC
            LIMIT 10
        """)
        
        # Store analysis results
        analysis = {
            'timestamp': datetime.now().isoformat(),
            'patterns': patterns,
            'top_fraud_reasons': fraud_reasons
        }
        
        # Save analysis to file
        import json
        analysis_path = f"/opt/airflow/logs/fraud_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(analysis_path, 'w') as f:
            json.dump(analysis, f, indent=2, default=str)
        
        logger.info(f"Fraud pattern analysis saved: {analysis_path}")
        
        snowflake.disconnect()
        
    except Exception as e:
        logger.error(f"Fraud pattern analysis error: {e}")
        raise

def check_system_performance():
    """Check fraud detection system performance metrics."""
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
        
        # Check processing latency
        latency_check = snowflake.execute_query("""
            SELECT 
                AVG(TIMESTAMPDIFF(second, transaction_timestamp, alert_timestamp)) as avg_latency_seconds,
                MAX(TIMESTAMPDIFF(second, transaction_timestamp, alert_timestamp)) as max_latency_seconds,
                COUNT(*) as processed_transactions
            FROM fraud_alerts fa
            JOIN raw_transactions rt ON fa.transaction_id = rt.transaction_id
            WHERE fa.alert_timestamp >= CURRENT_TIMESTAMP() - INTERVAL '1 hour'
        """)
        
        if latency_check:
            metrics = latency_check[0]
            logger.info(f"Performance metrics: {metrics}")
            
            # Check for performance issues
            if metrics['AVG_LATENCY_SECONDS'] > 60:
                raise Exception(f"High average processing latency: {metrics['AVG_LATENCY_SECONDS']} seconds")
            
            if metrics['MAX_LATENCY_SECONDS'] > 300:
                raise Exception(f"Very high max processing latency: {metrics['MAX_LATENCY_SECONDS']} seconds")
        
        snowflake.disconnect()
        
    except Exception as e:
        logger.error(f"System performance check error: {e}")
        raise

def generate_fraud_report():
    """Generate comprehensive fraud detection report."""
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
        
        # Generate comprehensive report
        report = {
            'timestamp': datetime.now().isoformat(),
            'summary': {},
            'hourly_breakdown': [],
            'top_fraud_users': [],
            'geographic_analysis': [],
            'device_analysis': []
        }
        
        # Get summary statistics
        summary = snowflake.execute_query("""
            SELECT 
                COUNT(*) as total_alerts,
                COUNT(CASE WHEN alert_level = 'CRITICAL' THEN 1 END) as critical_alerts,
                COUNT(CASE WHEN alert_level = 'HIGH' THEN 1 END) as high_alerts,
                COUNT(CASE WHEN alert_level = 'MEDIUM' THEN 1 END) as medium_alerts,
                COUNT(CASE WHEN alert_level = 'LOW' THEN 1 END) as low_alerts,
                AVG(fraud_score) as avg_fraud_score,
                COUNT(DISTINCT user_id) as affected_users,
                COUNT(DISTINCT DATE(alert_timestamp)) as active_days
            FROM fraud_alerts
            WHERE alert_timestamp >= CURRENT_TIMESTAMP() - INTERVAL '24 hours'
        """)
        
        if summary:
            report['summary'] = summary[0]
        
        # Get hourly breakdown
        hourly_breakdown = snowflake.execute_query("""
            SELECT 
                HOUR(alert_timestamp) as hour,
                COUNT(*) as alert_count,
                AVG(fraud_score) as avg_score
            FROM fraud_alerts
            WHERE alert_timestamp >= CURRENT_TIMESTAMP() - INTERVAL '24 hours'
            GROUP BY HOUR(alert_timestamp)
            ORDER BY hour
        """)
        report['hourly_breakdown'] = hourly_breakdown
        
        # Get top fraud users
        top_fraud_users = snowflake.execute_query("""
            SELECT 
                user_id,
                COUNT(*) as alert_count,
                AVG(fraud_score) as avg_fraud_score,
                MAX(amount) as max_transaction_amount,
                SUM(amount) as total_fraudulent_amount
            FROM fraud_alerts
            WHERE alert_timestamp >= CURRENT_TIMESTAMP() - INTERVAL '24 hours'
            GROUP BY user_id
            ORDER BY alert_count DESC
            LIMIT 20
        """)
        report['top_fraud_users'] = top_fraud_users
        
        # Geographic analysis
        geo_analysis = snowflake.execute_query("""
            SELECT 
                rt.location:country::STRING as country,
                COUNT(*) as alert_count,
                AVG(fa.fraud_score) as avg_fraud_score
            FROM fraud_alerts fa
            JOIN raw_transactions rt ON fa.transaction_id = rt.transaction_id
            WHERE fa.alert_timestamp >= CURRENT_TIMESTAMP() - INTERVAL '24 hours'
            GROUP BY rt.location:country::STRING
            ORDER BY alert_count DESC
            LIMIT 10
        """)
        report['geographic_analysis'] = geo_analysis
        
        # Device analysis
        device_analysis = snowflake.execute_query("""
            SELECT 
                rt.device_type,
                rt.browser,
                COUNT(*) as alert_count,
                AVG(fa.fraud_score) as avg_fraud_score
            FROM fraud_alerts fa
            JOIN raw_transactions rt ON fa.transaction_id = rt.transaction_id
            WHERE fa.alert_timestamp >= CURRENT_TIMESTAMP() - INTERVAL '24 hours'
            GROUP BY rt.device_type, rt.browser
            ORDER BY alert_count DESC
            LIMIT 10
        """)
        report['device_analysis'] = device_analysis
        
        # Save report
        import json
        report_path = f"/opt/airflow/logs/fraud_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        logger.info(f"Fraud detection report generated: {report_path}")
        
        snowflake.disconnect()
        
    except Exception as e:
        logger.error(f"Fraud report generation error: {e}")
        raise

def send_fraud_alert_email():
    """Send email alert if fraud detection issues are found."""
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
        
        # Check for critical alerts in the last hour
        critical_alerts = snowflake.execute_query("""
            SELECT 
                COUNT(*) as critical_count,
                COUNT(DISTINCT user_id) as affected_users,
                MAX(amount) as max_amount
            FROM fraud_alerts
            WHERE alert_level = 'CRITICAL' 
            AND alert_timestamp >= CURRENT_TIMESTAMP() - INTERVAL '1 hour'
        """)
        
        if critical_alerts and critical_alerts[0]['CRITICAL_COUNT'] > 5:
            # Send email alert
            email_content = f"""
            <h2>🚨 CRITICAL FRAUD ALERT</h2>
            <p>High number of critical fraud alerts detected in the last hour:</p>
            <ul>
                <li>Critical Alerts: {critical_alerts[0]['CRITICAL_COUNT']}</li>
                <li>Affected Users: {critical_alerts[0]['AFFECTED_USERS']}</li>
                <li>Max Transaction Amount: ${critical_alerts[0]['MAX_AMOUNT']}</li>
            </ul>
            <p>Please investigate immediately.</p>
            <p>Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            """
            
            # This would trigger an email notification
            logger.warning(f"CRITICAL FRAUD ALERT: {critical_alerts[0]['CRITICAL_COUNT']} alerts in last hour")
        
        snowflake.disconnect()
        
    except Exception as e:
        logger.error(f"Fraud alert email error: {e}")
        raise

# Define tasks
start_monitoring = DummyOperator(
    task_id='start_monitoring',
    dag=dag
)

fraud_health_check = PythonOperator(
    task_id='fraud_health_check',
    python_callable=check_fraud_detection_health,
    dag=dag
)

pattern_analysis = PythonOperator(
    task_id='pattern_analysis',
    python_callable=analyze_fraud_patterns,
    dag=dag
)

performance_check = PythonOperator(
    task_id='performance_check',
    python_callable=check_system_performance,
    dag=dag
)

generate_report = PythonOperator(
    task_id='generate_fraud_report',
    python_callable=generate_fraud_report,
    dag=dag
)

send_alerts = PythonOperator(
    task_id='send_fraud_alerts',
    python_callable=send_fraud_alert_email,
    dag=dag
)

end_monitoring = DummyOperator(
    task_id='end_monitoring',
    dag=dag
)

# Define task dependencies
start_monitoring >> fraud_health_check

fraud_health_check >> [pattern_analysis, performance_check]

[pattern_analysis, performance_check] >> generate_report

generate_report >> send_alerts

send_alerts >> end_monitoring
