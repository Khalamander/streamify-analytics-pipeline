#!/usr/bin/env python3
"""
Streamify Analytics Pipeline - Web Dashboard
Real-time visualization of the analytics pipeline
"""

from flask import Flask, render_template, jsonify, request
import json
import random
import time
import threading
from datetime import datetime, timezone, timedelta
from collections import deque
import os

app = Flask(__name__)

# Global data storage
transactions_data = deque(maxlen=100)  # Keep last 100 transactions
fraud_alerts = deque(maxlen=50)       # Keep last 50 fraud alerts
analytics_data = {
    'total_revenue': 0,
    'total_transactions': 0,
    'fraud_count': 0,
    'avg_transaction': 0,
    'category_breakdown': {},
    'payment_methods': {},
    'countries': {},
    'processing_rate': 0
}

# Data generation settings
is_generating = False
generation_thread = None

def generate_transaction():
    """Generate a realistic transaction."""
    categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports', 'Beauty', 'Toys', 'Automotive']
    payment_methods = ['credit_card', 'debit_card', 'paypal', 'apple_pay', 'google_pay', 'cryptocurrency']
    countries = ['US', 'CA', 'UK', 'DE', 'FR', 'AU', 'JP', 'BR']
    cities = ['New York', 'London', 'Berlin', 'Toronto', 'Paris', 'Sydney', 'Tokyo', 'São Paulo']
    
    # Generate realistic amounts with some high-value transactions
    if random.random() < 0.05:  # 5% chance of high-value transaction
        amount = round(random.uniform(2000, 10000), 2)
    else:
        amount = round(random.uniform(10, 500), 2)
    
    transaction = {
        'transaction_id': f"txn_{random.randint(100000, 999999)}",
        'user_id': f"user_{random.randint(1000, 9999)}",
        'amount': amount,
        'product_category': random.choice(categories),
        'payment_method': random.choice(payment_methods),
        'status': 'completed',
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'location': {
            'country': random.choice(countries),
            'city': random.choice(cities)
        }
    }
    
    return transaction

def detect_fraud(transaction):
    """Detect fraud in transaction."""
    fraud_score = 0.0
    reasons = []
    
    # High amount check
    if transaction['amount'] > 2000:
        fraud_score += 0.7
        reasons.append("High amount transaction")
    
    # Suspicious payment method
    if transaction['payment_method'] == 'cryptocurrency':
        fraud_score += 0.5
        reasons.append("Suspicious payment method")
    
    # Suspicious product category
    if transaction['product_category'] in ['gift_cards', 'prepaid_cards']:
        fraud_score += 0.3
        reasons.append("Suspicious product category")
    
    # Multiple transactions from same user in short time
    recent_transactions = [t for t in transactions_data if t['user_id'] == transaction['user_id']]
    if len(recent_transactions) > 3:
        fraud_score += 0.4
        reasons.append("Multiple rapid transactions")
    
    return {
        'fraud_score': fraud_score,
        'is_fraud': fraud_score > 0.5,
        'reasons': reasons
    }

def update_analytics(transaction, fraud_result):
    """Update analytics data."""
    global analytics_data
    
    # Update basic metrics
    analytics_data['total_revenue'] += transaction['amount']
    analytics_data['total_transactions'] += 1
    analytics_data['avg_transaction'] = analytics_data['total_revenue'] / analytics_data['total_transactions']
    
    if fraud_result['is_fraud']:
        analytics_data['fraud_count'] += 1
    
    # Update category breakdown
    category = transaction['product_category']
    if category not in analytics_data['category_breakdown']:
        analytics_data['category_breakdown'][category] = 0
    analytics_data['category_breakdown'][category] += transaction['amount']
    
    # Update payment methods
    method = transaction['payment_method']
    if method not in analytics_data['payment_methods']:
        analytics_data['payment_methods'][method] = 0
    analytics_data['payment_methods'][method] += 1
    
    # Update countries
    country = transaction['location']['country']
    if country not in analytics_data['countries']:
        analytics_data['countries'][country] = 0
    analytics_data['countries'][country] += 1

def data_generation_worker():
    """Background worker to generate data."""
    global is_generating, analytics_data
    
    while is_generating:
        # Generate transaction
        transaction = generate_transaction()
        
        # Detect fraud
        fraud_result = detect_fraud(transaction)
        
        # Add to data storage
        transactions_data.append(transaction)
        
        # Add fraud alert if detected
        if fraud_result['is_fraud']:
            fraud_alert = {
                'transaction_id': transaction['transaction_id'],
                'amount': transaction['amount'],
                'fraud_score': fraud_result['fraud_score'],
                'reasons': fraud_result['reasons'],
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
            fraud_alerts.append(fraud_alert)
        
        # Update analytics
        update_analytics(transaction, fraud_result)
        
        # Calculate processing rate (transactions per minute)
        if len(transactions_data) > 1:
            time_diff = (datetime.now(timezone.utc) - datetime.fromisoformat(transactions_data[0]['timestamp'].replace('Z', '+00:00'))).total_seconds()
            if time_diff > 0:
                analytics_data['processing_rate'] = len(transactions_data) / (time_diff / 60)
        
        # Sleep for random interval (simulate real-time data)
        time.sleep(random.uniform(0.5, 2.0))

@app.route('/')
def dashboard():
    """Main dashboard page."""
    return render_template('dashboard.html')

@app.route('/api/transactions')
def get_transactions():
    """Get recent transactions."""
    return jsonify(list(transactions_data))

@app.route('/api/fraud_alerts')
def get_fraud_alerts():
    """Get fraud alerts."""
    return jsonify(list(fraud_alerts))

@app.route('/api/analytics')
def get_analytics():
    """Get analytics data."""
    return jsonify(analytics_data)

@app.route('/api/start_generation', methods=['POST'])
def start_generation():
    """Start data generation."""
    global is_generating, generation_thread
    
    if not is_generating:
        is_generating = True
        generation_thread = threading.Thread(target=data_generation_worker)
        generation_thread.daemon = True
        generation_thread.start()
        return jsonify({'status': 'started'})
    
    return jsonify({'status': 'already_running'})

@app.route('/api/stop_generation', methods=['POST'])
def stop_generation():
    """Stop data generation."""
    global is_generating
    
    is_generating = False
    return jsonify({'status': 'stopped'})

@app.route('/api/reset_data', methods=['POST'])
def reset_data():
    """Reset all data."""
    global transactions_data, fraud_alerts, analytics_data
    
    transactions_data.clear()
    fraud_alerts.clear()
    analytics_data = {
        'total_revenue': 0,
        'total_transactions': 0,
        'fraud_count': 0,
        'avg_transaction': 0,
        'category_breakdown': {},
        'payment_methods': {},
        'countries': {},
        'processing_rate': 0
    }
    
    return jsonify({'status': 'reset'})

if __name__ == '__main__':
    # Create templates directory if it doesn't exist
    os.makedirs('templates', exist_ok=True)
    
    print("🚀 Starting Streamify Analytics Pipeline Dashboard...")
    print("📊 Open your browser to: http://localhost:8080")
    print("🔄 Use the controls to start/stop data generation")
    
    app.run(debug=True, host='0.0.0.0', port=8080)
