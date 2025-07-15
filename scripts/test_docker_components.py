#!/usr/bin/env python3
"""
Test Docker Components
Tests the Docker services for the Streamify Analytics Pipeline
"""

import subprocess
import time
import requests
import json
from datetime import datetime

def run_command(command, check=True):
    """Run a shell command and return the result."""
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        if check and result.returncode != 0:
            print(f"❌ Command failed: {command}")
            print(f"Error: {result.stderr}")
            return False
        return result
    except Exception as e:
        print(f"❌ Error running command: {e}")
        return None

def check_docker_status():
    """Check if Docker is running."""
    print("🔍 Checking Docker status...")
    
    result = run_command('docker ps', check=False)
    if result and result.returncode == 0:
        print("✅ Docker is running")
        return True
    else:
        print("❌ Docker is not running")
        print("Please start Docker Desktop and try again")
        return False

def start_kafka_services():
    """Start Kafka and Zookeeper services."""
    print("\n🚀 Starting Kafka services...")
    
    # Start Zookeeper and Kafka
    result = run_command('docker-compose up -d zookeeper kafka')
    if result and result.returncode == 0:
        print("✅ Kafka services started")
        
        # Wait for services to be ready
        print("⏳ Waiting for services to be ready...")
        time.sleep(30)
        
        # Check if services are running
        result = run_command('docker-compose ps')
        if result and result.returncode == 0:
            print("📊 Service Status:")
            print(result.stdout)
            return True
        else:
            print("❌ Failed to check service status")
            return False
    else:
        print("❌ Failed to start Kafka services")
        return False

def test_kafka_connection():
    """Test Kafka connection."""
    print("\n🔌 Testing Kafka connection...")
    
    # Check if Kafka is accessible
    result = run_command('docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list')
    if result and result.returncode == 0:
        print("✅ Kafka connection successful")
        print(f"📋 Available topics: {result.stdout.strip()}")
        return True
    else:
        print("❌ Kafka connection failed")
        return False

def create_kafka_topics():
    """Create required Kafka topics."""
    print("\n📝 Creating Kafka topics...")
    
    topics = ['sales_stream', 'fraud_alerts', 'analytics_stream']
    
    for topic in topics:
        result = run_command(f'docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic {topic} --partitions 3 --replication-factor 1')
        if result and result.returncode == 0:
            print(f"✅ Created topic: {topic}")
        else:
            print(f"❌ Failed to create topic: {topic}")
    
    # List all topics
    result = run_command('docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list')
    if result and result.returncode == 0:
        print(f"\n📋 All topics: {result.stdout.strip()}")

def test_web_dashboard():
    """Test the web dashboard."""
    print("\n🌐 Testing web dashboard...")
    
    try:
        response = requests.get('http://localhost:8080', timeout=5)
        if response.status_code == 200:
            print("✅ Web dashboard is accessible")
            print("🌐 Open your browser to: http://localhost:8080")
            return True
        else:
            print(f"❌ Web dashboard returned status code: {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"❌ Web dashboard not accessible: {e}")
        return False

def test_data_generation():
    """Test data generation through the web API."""
    print("\n📊 Testing data generation...")
    
    try:
        # Start data generation
        response = requests.post('http://localhost:8080/api/start_generation', timeout=5)
        if response.status_code == 200:
            print("✅ Data generation started")
            
            # Wait a bit for data to generate
            time.sleep(5)
            
            # Check analytics
            response = requests.get('http://localhost:8080/api/analytics', timeout=5)
            if response.status_code == 200:
                analytics = response.json()
                print(f"📈 Analytics data: {analytics['total_transactions']} transactions, ${analytics['total_revenue']:.2f} revenue")
                return True
            else:
                print("❌ Failed to fetch analytics")
                return False
        else:
            print(f"❌ Failed to start data generation: {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"❌ Data generation test failed: {e}")
        return False

def cleanup_services():
    """Clean up Docker services."""
    print("\n🧹 Cleaning up services...")
    
    result = run_command('docker-compose down')
    if result and result.returncode == 0:
        print("✅ Services cleaned up")
    else:
        print("❌ Failed to clean up services")

def main():
    """Main test function."""
    print("🚀 Streamify Analytics Pipeline - Docker Component Test")
    print("=" * 60)
    
    # Check Docker status
    if not check_docker_status():
        return 1
    
    # Start Kafka services
    if not start_kafka_services():
        print("❌ Failed to start Kafka services")
        return 1
    
    # Test Kafka connection
    if not test_kafka_connection():
        print("❌ Kafka connection failed")
        cleanup_services()
        return 1
    
    # Create topics
    create_kafka_topics()
    
    # Test web dashboard
    if not test_web_dashboard():
        print("❌ Web dashboard not accessible")
        print("💡 Make sure to run 'python app.py' in another terminal")
        cleanup_services()
        return 1
    
    # Test data generation
    if not test_data_generation():
        print("❌ Data generation test failed")
        cleanup_services()
        return 1
    
    print("\n🎉 All Docker component tests passed!")
    print("✅ Your pipeline is fully functional with Docker services")
    print("🌐 Web dashboard: http://localhost:8080")
    print("📊 Kafka services: Running on localhost:9092")
    
    print("\n💡 To stop services, run: docker-compose down")
    
    return 0

if __name__ == "__main__":
    exit(main())
