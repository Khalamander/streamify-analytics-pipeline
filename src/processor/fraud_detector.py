"""
Advanced Fraud Detection Module
Week 4: Fraud Detection Logic and Alert System

This module implements sophisticated fraud detection algorithms including
machine learning-based scoring and real-time alerting.
"""

import json
import logging
import numpy as np
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Tuple
from dataclasses import dataclass
from collections import defaultdict, deque
import pickle
import os

@dataclass
class FraudAlert:
    """Data class for fraud alerts."""
    transaction_id: str
    user_id: str
    amount: float
    fraud_reason: str
    fraud_score: float
    timestamp: datetime
    alert_level: str  # LOW, MEDIUM, HIGH, CRITICAL
    additional_context: Dict[str, Any]

class FraudDetector:
    """Advanced fraud detection system with multiple detection algorithms."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Fraud detection thresholds
        self.thresholds = {
            'high_amount': 2000.0,
            'very_high_amount': 5000.0,
            'rapid_transactions_count': 3,
            'rapid_transactions_window': 5,  # seconds
            'velocity_threshold': 1000.0,  # dollars per minute
            'geographic_anomaly_threshold': 0.8,
            'device_anomaly_threshold': 0.7
        }
        
        # User behavior tracking
        self.user_behavior = defaultdict(lambda: {
            'transaction_history': deque(maxlen=100),
            'amount_history': deque(maxlen=100),
            'location_history': deque(maxlen=50),
            'device_history': deque(maxlen=20),
            'last_transaction_time': None,
            'total_spent_today': 0.0,
            'transaction_count_today': 0
        })
        
        # Risk scoring weights
        self.risk_weights = {
            'amount_risk': 0.3,
            'velocity_risk': 0.25,
            'geographic_risk': 0.2,
            'device_risk': 0.15,
            'pattern_risk': 0.1
        }
    
    def calculate_amount_risk_score(self, amount: float, user_id: str) -> float:
        """Calculate risk score based on transaction amount."""
        user_data = self.user_behavior[user_id]
        
        if not user_data['amount_history']:
            # First transaction - moderate risk for high amounts
            if amount > self.thresholds['very_high_amount']:
                return 0.8
            elif amount > self.thresholds['high_amount']:
                return 0.5
            else:
                return 0.1
        
        # Calculate statistical risk based on historical amounts
        amounts = list(user_data['amount_history'])
        mean_amount = np.mean(amounts)
        std_amount = np.std(amounts)
        
        if std_amount == 0:
            # No variance in historical amounts
            if amount > mean_amount * 2:
                return 0.9
            elif amount > mean_amount * 1.5:
                return 0.6
            else:
                return 0.2
        
        # Z-score based risk
        z_score = (amount - mean_amount) / std_amount
        
        if z_score > 3:
            return 0.95
        elif z_score > 2:
            return 0.8
        elif z_score > 1:
            return 0.5
        else:
            return 0.2
    
    def calculate_velocity_risk_score(self, amount: float, user_id: str, timestamp: datetime) -> float:
        """Calculate risk score based on spending velocity."""
        user_data = self.user_behavior[user_id]
        
        if not user_data['last_transaction_time']:
            return 0.1
        
        # Calculate time since last transaction
        time_diff = (timestamp - user_data['last_transaction_time']).total_seconds()
        
        if time_diff < 60:  # Less than 1 minute
            # Very high velocity
            velocity = amount / (time_diff / 60)  # dollars per minute
            if velocity > self.thresholds['velocity_threshold'] * 2:
                return 0.95
            elif velocity > self.thresholds['velocity_threshold']:
                return 0.8
            else:
                return 0.6
        elif time_diff < 300:  # Less than 5 minutes
            velocity = amount / (time_diff / 60)
            if velocity > self.thresholds['velocity_threshold']:
                return 0.7
            else:
                return 0.3
        else:
            return 0.1
    
    def calculate_geographic_risk_score(self, location: Dict[str, Any], user_id: str) -> float:
        """Calculate risk score based on geographic anomalies."""
        user_data = self.user_behavior[user_id]
        
        if not user_data['location_history']:
            return 0.2  # New location, moderate risk
        
        # Check for impossible travel
        current_lat = location.get('latitude')
        current_lon = location.get('longitude')
        
        if current_lat is None or current_lon is None:
            return 0.5  # Missing location data
        
        # Calculate distance from last known location
        last_location = user_data['location_history'][-1]
        last_lat = last_location.get('latitude')
        last_lon = last_location.get('longitude')
        
        if last_lat is None or last_lon is None:
            return 0.3
        
        # Simple distance calculation (not accounting for Earth's curvature)
        distance = np.sqrt(
            (current_lat - last_lat) ** 2 + (current_lon - last_lon) ** 2
        )
        
        # Time since last transaction
        if user_data['last_transaction_time']:
            time_diff = (datetime.now(timezone.utc) - user_data['last_transaction_time']).total_seconds()
            
            # Rough speed calculation (degrees per second)
            if time_diff > 0:
                speed = distance / time_diff
                
                # If speed > 0.01 degrees per second, it's suspicious
                # (roughly equivalent to > 1000 km/h)
                if speed > 0.01:
                    return 0.9
                elif speed > 0.005:
                    return 0.6
                else:
                    return 0.2
        
        return 0.3
    
    def calculate_device_risk_score(self, device_info: Dict[str, str], user_id: str) -> float:
        """Calculate risk score based on device anomalies."""
        user_data = self.user_behavior[user_id]
        
        if not user_data['device_history']:
            return 0.2  # New device, moderate risk
        
        # Check for device changes
        current_device = f"{device_info.get('device_type', 'unknown')}_{device_info.get('browser', 'unknown')}"
        last_device = user_data['device_history'][-1]
        
        if current_device != last_device:
            # Device change detected
            device_change_frequency = len(set(user_data['device_history'])) / len(user_data['device_history'])
            
            if device_change_frequency > 0.5:  # User changes devices frequently
                return 0.4
            else:
                return 0.7  # Unusual device change
        else:
            return 0.1  # Same device, low risk
    
    def calculate_pattern_risk_score(self, transaction: Dict[str, Any], user_id: str) -> float:
        """Calculate risk score based on transaction patterns."""
        user_data = self.user_behavior[user_id]
        
        # Check for unusual payment methods
        payment_method = transaction.get('payment_method', '')
        if payment_method in ['cryptocurrency', 'wire_transfer']:
            return 0.8
        
        # Check for unusual product categories
        category = transaction.get('product_category', '')
        if category in ['gift_cards', 'prepaid_cards']:
            return 0.6
        
        # Check for round number amounts (potential test transactions)
        amount = transaction.get('amount', 0)
        if amount in [1.0, 10.0, 100.0, 1000.0]:
            return 0.4
        
        # Check for very small amounts (potential probing)
        if amount < 1.0:
            return 0.5
        
        return 0.1
    
    def update_user_behavior(self, transaction: Dict[str, Any], user_id: str, timestamp: datetime):
        """Update user behavior tracking data."""
        user_data = self.user_behavior[user_id]
        
        # Update transaction history
        user_data['transaction_history'].append(transaction)
        user_data['amount_history'].append(transaction.get('amount', 0))
        user_data['location_history'].append(transaction.get('location', {}))
        user_data['device_history'].append(
            f"{transaction.get('device_type', 'unknown')}_{transaction.get('browser', 'unknown')}"
        )
        user_data['last_transaction_time'] = timestamp
        
        # Update daily totals
        if user_data['last_transaction_time']:
            last_date = user_data['last_transaction_time'].date()
            current_date = timestamp.date()
            
            if last_date != current_date:
                # New day, reset counters
                user_data['total_spent_today'] = 0.0
                user_data['transaction_count_today'] = 0
        
        user_data['total_spent_today'] += transaction.get('amount', 0)
        user_data['transaction_count_today'] += 1
    
    def detect_fraud(self, transaction: Dict[str, Any]) -> FraudAlert:
        """Main fraud detection method."""
        user_id = transaction.get('user_id', '')
        amount = transaction.get('amount', 0)
        timestamp = datetime.now(timezone.utc)
        
        # Calculate individual risk scores
        amount_risk = self.calculate_amount_risk_score(amount, user_id)
        velocity_risk = self.calculate_velocity_risk_score(amount, user_id, timestamp)
        geographic_risk = self.calculate_geographic_risk_score(
            transaction.get('location', {}), user_id
        )
        device_risk = self.calculate_device_risk_score(
            {
                'device_type': transaction.get('device_type', ''),
                'browser': transaction.get('browser', '')
            },
            user_id
        )
        pattern_risk = self.calculate_pattern_risk_score(transaction, user_id)
        
        # Calculate weighted overall risk score
        overall_risk = (
            amount_risk * self.risk_weights['amount_risk'] +
            velocity_risk * self.risk_weights['velocity_risk'] +
            geographic_risk * self.risk_weights['geographic_risk'] +
            device_risk * self.risk_weights['device_risk'] +
            pattern_risk * self.risk_weights['pattern_risk']
        )
        
        # Determine alert level
        if overall_risk >= 0.8:
            alert_level = "CRITICAL"
        elif overall_risk >= 0.6:
            alert_level = "HIGH"
        elif overall_risk >= 0.4:
            alert_level = "MEDIUM"
        else:
            alert_level = "LOW"
        
        # Generate fraud reason
        fraud_reasons = []
        if amount_risk > 0.7:
            fraud_reasons.append("Unusually high transaction amount")
        if velocity_risk > 0.7:
            fraud_reasons.append("High spending velocity")
        if geographic_risk > 0.7:
            fraud_reasons.append("Suspicious geographic location")
        if device_risk > 0.7:
            fraud_reasons.append("Unusual device or browser")
        if pattern_risk > 0.7:
            fraud_reasons.append("Suspicious transaction pattern")
        
        fraud_reason = "; ".join(fraud_reasons) if fraud_reasons else "Multiple risk factors detected"
        
        # Create fraud alert
        alert = FraudAlert(
            transaction_id=transaction.get('transaction_id', ''),
            user_id=user_id,
            amount=amount,
            fraud_reason=fraud_reason,
            fraud_score=overall_risk,
            timestamp=timestamp,
            alert_level=alert_level,
            additional_context={
                'amount_risk': amount_risk,
                'velocity_risk': velocity_risk,
                'geographic_risk': geographic_risk,
                'device_risk': device_risk,
                'pattern_risk': pattern_risk,
                'user_transaction_count_today': self.user_behavior[user_id]['transaction_count_today'],
                'user_total_spent_today': self.user_behavior[user_id]['total_spent_today']
            }
        )
        
        # Update user behavior
        self.update_user_behavior(transaction, user_id, timestamp)
        
        return alert
    
    def should_alert(self, alert: FraudAlert) -> bool:
        """Determine if an alert should be sent based on risk score and level."""
        return alert.fraud_score >= 0.4 or alert.alert_level in ["HIGH", "CRITICAL"]
    
    def get_alert_summary(self) -> Dict[str, Any]:
        """Get summary of fraud detection statistics."""
        total_users = len(self.user_behavior)
        total_transactions = sum(
            len(user_data['transaction_history']) 
            for user_data in self.user_behavior.values()
        )
        
        return {
            'total_users_tracked': total_users,
            'total_transactions_processed': total_transactions,
            'detection_thresholds': self.thresholds,
            'risk_weights': self.risk_weights
        }
