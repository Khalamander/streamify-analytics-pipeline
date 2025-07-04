# Streamify Analytics Pipeline - Deployment Guide

## Overview

This guide provides comprehensive instructions for deploying the Streamify Analytics Pipeline in various environments, from local development to production AWS infrastructure.

## Prerequisites

### System Requirements
- **Operating System**: Linux (Ubuntu 20.04+), macOS, or Windows with WSL2
- **Python**: 3.8 or higher
- **Docker**: 20.10+ with Docker Compose
- **Memory**: Minimum 8GB RAM (16GB recommended)
- **Storage**: Minimum 50GB free space
- **Network**: Internet connection for package downloads

### Cloud Requirements
- **AWS Account**: With appropriate permissions
- **Snowflake Account**: Free trial available
- **Domain**: Optional, for production deployment

## Environment Setup

### 1. Local Development Environment

#### Clone Repository
```bash
git clone https://github.com/your-username/streamify-analytics-pipeline.git
cd streamify-analytics-pipeline
```

#### Python Environment
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

#### Environment Configuration
```bash
# Copy environment template
cp env.example .env

# Edit configuration
nano .env
```

Required environment variables:
```bash
# AWS Configuration
AWS_ACCESS_KEY_ID=your_access_key_here
AWS_SECRET_ACCESS_KEY=your_secret_key_here
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET_NAME=streamify-analytics-data-lake

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_SALES=sales_stream
KAFKA_TOPIC_FRAUD=fraud_alerts

# Snowflake Configuration
SNOWFLAKE_ACCOUNT=your_account.snowflakecomputing.com
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=STREAMIFY_ANALYTICS
SNOWFLAKE_SCHEMA=PUBLIC
```

### 2. AWS Infrastructure Setup

#### S3 Bucket Creation
```bash
# Create S3 bucket
aws s3 mb s3://streamify-analytics-data-lake

# Configure bucket policies
aws s3api put-bucket-versioning \
    --bucket streamify-analytics-data-lake \
    --versioning-configuration Status=Enabled

# Set up lifecycle policies
aws s3api put-bucket-lifecycle-configuration \
    --bucket streamify-analytics-data-lake \
    --lifecycle-configuration file://s3-lifecycle.json
```

#### IAM Role Creation
```bash
# Create IAM role for Glue
aws iam create-role \
    --role-name GlueServiceRole-StreamifyAnalytics \
    --assume-role-policy-document file://glue-trust-policy.json

# Attach policies
aws iam attach-role-policy \
    --role-name GlueServiceRole-StreamifyAnalytics \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole

aws iam attach-role-policy \
    --role-name GlueServiceRole-StreamifyAnalytics \
    --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
```

### 3. Snowflake Setup

#### Account Configuration
1. Sign up for Snowflake free trial
2. Create a new warehouse: `COMPUTE_WH`
3. Create a new database: `STREAMIFY_ANALYTICS`
4. Create a new schema: `PUBLIC`
5. Create a service user with appropriate permissions

#### Database Schema Creation
```sql
-- Run in Snowflake console
USE DATABASE STREAMIFY_ANALYTICS;
USE SCHEMA PUBLIC;

-- Create tables (run setup_snowflake.py)
python scripts/setup_snowflake.py
```

## Deployment Methods

### Method 1: Docker Compose (Recommended for Development)

#### Start Services
```bash
# Start all services
docker-compose up -d

# Check service status
docker-compose ps

# View logs
docker-compose logs -f
```

#### Service URLs
- **Airflow UI**: http://localhost:8080 (admin/admin)
- **Kafka Manager**: http://localhost:9000
- **Zookeeper**: localhost:2181

#### Initialize Pipeline
```bash
# Setup Kafka topics
python scripts/setup_kafka_topics.py

# Test data generation
python src/producer/producer.py --duration 5

# Test stream processing
python src/processor/stream_processor.py
```

### Method 2: Manual Installation (Production)

#### Install Dependencies
```bash
# Install system packages
sudo apt-get update
sudo apt-get install -y openjdk-11-jdk python3-pip

# Install Python packages
pip3 install -r requirements.txt

# Install Kafka
wget https://downloads.apache.org/kafka/2.8.1/kafka_2.13-2.8.1.tgz
tar -xzf kafka_2.13-2.8.1.tgz
sudo mv kafka_2.13-2.8.1 /opt/kafka
```

#### Configure Services
```bash
# Configure Kafka
sudo nano /opt/kafka/config/server.properties

# Configure Zookeeper
sudo nano /opt/kafka/config/zookeeper.properties

# Start services
sudo /opt/kafka/bin/zookeeper-server-start.sh -daemon /opt/kafka/config/zookeeper.properties
sudo /opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/server.properties
```

#### Install Airflow
```bash
# Set Airflow home
export AIRFLOW_HOME=~/airflow

# Initialize Airflow
airflow db init
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

# Start Airflow
airflow webserver --port 8080 &
airflow scheduler &
```

### Method 3: AWS EC2 Deployment

#### Launch EC2 Instance
```bash
# Launch t3.large instance
aws ec2 run-instances \
    --image-id ami-0c02fb55956c7d316 \
    --instance-type t3.large \
    --key-name your-key-pair \
    --security-group-ids sg-xxxxxxxxx \
    --subnet-id subnet-xxxxxxxxx \
    --user-data file://user-data.sh
```

#### Instance Configuration
```bash
#!/bin/bash
# user-data.sh
yum update -y
yum install -y docker git python3-pip

# Start Docker
systemctl start docker
systemctl enable docker
usermod -a -G docker ec2-user

# Clone repository
git clone https://github.com/your-username/streamify-analytics-pipeline.git
cd streamify-analytics-pipeline

# Install dependencies
pip3 install -r requirements.txt

# Start services
docker-compose up -d
```

## Configuration Management

### Environment-Specific Configs

#### Development
```yaml
# docker-compose.dev.yml
version: '3.8'
services:
  kafka:
    ports:
      - "9092:9092"
    environment:
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT_HOST://localhost:9092
```

#### Production
```yaml
# docker-compose.prod.yml
version: '3.8'
services:
  kafka:
    environment:
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT_HOST://kafka.internal:9092
    deploy:
      resources:
        limits:
          memory: 2G
          cpus: '1.0'
```

### Secrets Management

#### Using AWS Secrets Manager
```python
import boto3
import json

def get_secret(secret_name):
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response['SecretString'])

# Usage
secrets = get_secret('streamify-analytics-secrets')
SNOWFLAKE_PASSWORD = secrets['snowflake_password']
```

#### Using Environment Variables
```bash
# Production environment
export SNOWFLAKE_PASSWORD=$(aws secretsmanager get-secret-value \
    --secret-id streamify-analytics-secrets \
    --query SecretString --output text | jq -r .snowflake_password)
```

## Monitoring and Logging

### Log Configuration
```python
# logging.conf
[loggers]
keys=root,streamify

[handlers]
keys=consoleHandler,fileHandler

[formatters]
keys=simpleFormatter

[logger_root]
level=INFO
handlers=consoleHandler

[logger_streamify]
level=INFO
handlers=fileHandler
qualname=streamify
propagate=0

[handler_consoleHandler]
class=StreamHandler
level=INFO
formatter=simpleFormatter
args=(sys.stdout,)

[handler_fileHandler]
class=FileHandler
level=INFO
formatter=simpleFormatter
args=('streamify.log',)

[formatter_simpleFormatter]
format=%(asctime)s - %(name)s - %(levelname)s - %(message)s
```

### Monitoring Setup
```bash
# Install monitoring tools
pip install prometheus-client grafana-api

# Start monitoring
python scripts/monitor_pipeline.py --interval 60
```

## Testing and Validation

### Pre-Deployment Testing
```bash
# Run comprehensive tests
python scripts/pipeline_test.py

# Test individual components
python -m pytest tests/unit/
python -m pytest tests/integration/

# Load testing
python scripts/load_test.py --duration 300
```

### Post-Deployment Validation
```bash
# Validate data flow
python scripts/validate_pipeline.py

# Check data quality
python scripts/data_quality_check.py

# Performance testing
python scripts/performance_test.py
```

## Troubleshooting

### Common Issues

#### Kafka Connection Issues
```bash
# Check Kafka status
docker-compose logs kafka

# Restart Kafka
docker-compose restart kafka

# Check topic creation
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

#### Spark Processing Issues
```bash
# Check Spark logs
docker-compose logs spark

# Restart Spark
docker-compose restart spark

# Check checkpoint directory
ls -la checkpoints/
```

#### Airflow DAG Issues
```bash
# Check Airflow logs
docker-compose logs airflow-scheduler

# Restart Airflow
docker-compose restart airflow-scheduler

# Check DAG status
curl http://localhost:8080/api/v1/dags/streamify_analytics_pipeline
```

### Performance Issues

#### High Memory Usage
```bash
# Check memory usage
docker stats

# Adjust memory limits
docker-compose down
# Edit docker-compose.yml
docker-compose up -d
```

#### Slow Processing
```bash
# Check Kafka lag
docker-compose exec kafka kafka-consumer-groups \
    --bootstrap-server localhost:9092 \
    --group streamify_analytics_group \
    --describe

# Optimize Spark configuration
export SPARK_DRIVER_MEMORY=2g
export SPARK_EXECUTOR_MEMORY=2g
```

## Maintenance and Updates

### Regular Maintenance
```bash
# Daily health checks
python scripts/health_check.py

# Weekly data cleanup
python scripts/cleanup_old_data.py

# Monthly performance review
python scripts/performance_report.py
```

### Updates and Upgrades
```bash
# Backup current configuration
cp docker-compose.yml docker-compose.yml.backup

# Pull latest changes
git pull origin main

# Update dependencies
pip install -r requirements.txt --upgrade

# Restart services
docker-compose down
docker-compose up -d
```

### Backup and Recovery
```bash
# Backup configuration
tar -czf streamify-backup-$(date +%Y%m%d).tar.gz \
    docker-compose.yml .env dags/ src/ scripts/

# Backup data
aws s3 sync s3://streamify-analytics-data-lake \
    s3://streamify-analytics-backup/$(date +%Y%m%d)/
```

## Security Considerations

### Network Security
```bash
# Configure firewall
sudo ufw allow 8080/tcp  # Airflow
sudo ufw allow 9092/tcp  # Kafka
sudo ufw deny 2181/tcp   # Zookeeper (internal only)
```

### Data Encryption
```bash
# Enable S3 encryption
aws s3api put-bucket-encryption \
    --bucket streamify-analytics-data-lake \
    --server-side-encryption-configuration '{
        "Rules": [{
            "ApplyServerSideEncryptionByDefault": {
                "SSEAlgorithm": "AES256"
            }
        }]
    }'
```

### Access Control
```bash
# Create IAM user for application
aws iam create-user --user-name streamify-app

# Attach minimal required policies
aws iam attach-user-policy \
    --user-name streamify-app \
    --policy-arn arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess
```

## Scaling Considerations

### Horizontal Scaling
```yaml
# docker-compose.scale.yml
version: '3.8'
services:
  kafka:
    deploy:
      replicas: 3
  spark:
    deploy:
      replicas: 2
```

### Vertical Scaling
```yaml
# Increase resource limits
services:
  kafka:
    deploy:
      resources:
        limits:
          memory: 4G
          cpus: '2.0'
```

## Production Checklist

- [ ] Environment variables configured
- [ ] AWS credentials set up
- [ ] Snowflake database created
- [ ] S3 bucket configured
- [ ] IAM roles created
- [ ] Security groups configured
- [ ] Monitoring set up
- [ ] Logging configured
- [ ] Backup strategy implemented
- [ ] Testing completed
- [ ] Documentation updated
- [ ] Team training completed

---

This deployment guide provides comprehensive instructions for deploying the Streamify Analytics Pipeline across different environments. Follow the appropriate method based on your requirements and infrastructure.
