"""
Real-Time Analytics Engine
Week 5: Live Analytics with Windowed Aggregations

This module provides comprehensive real-time analytics including
windowed aggregations, KPI calculations, and trend analysis.
"""

import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, asdict
from collections import defaultdict, deque
import statistics

@dataclass
class AnalyticsWindow:
    """Data class for analytics window results."""
    window_start: datetime
    window_end: datetime
    window_duration_seconds: int
    total_transactions: int
    total_sales: float
    average_transaction_amount: float
    unique_users: int
    unique_products: int
    unique_categories: int
    top_categories: List[Dict[str, Any]]
    top_products: List[Dict[str, Any]]
    payment_method_distribution: Dict[str, int]
    device_type_distribution: Dict[str, int]
    geographic_distribution: Dict[str, int]
    fraud_rate: float
    conversion_rate: float
    revenue_per_user: float
    transactions_per_user: float

@dataclass
class KPIMetrics:
    """Data class for KPI metrics."""
    timestamp: datetime
    total_revenue: float
    total_transactions: int
    average_order_value: float
    conversion_rate: float
    fraud_rate: float
    top_performing_category: str
    top_performing_product: str
    customer_acquisition_rate: float
    revenue_growth_rate: float
    transaction_velocity: float  # transactions per minute

class RealTimeAnalyticsEngine:
    """Real-time analytics engine for e-commerce metrics."""
    
    def __init__(self, window_size_seconds: int = 60, slide_interval_seconds: int = 30):
        self.logger = logging.getLogger(__name__)
        self.window_size_seconds = window_size_seconds
        self.slide_interval_seconds = slide_interval_seconds
        
        # Data storage for windowed analytics
        self.transaction_buffer = deque()
        self.user_sessions = defaultdict(list)
        self.product_performance = defaultdict(lambda: {
            'total_sales': 0.0,
            'transaction_count': 0,
            'unique_users': set(),
            'last_seen': None
        })
        self.category_performance = defaultdict(lambda: {
            'total_sales': 0.0,
            'transaction_count': 0,
            'unique_users': set(),
            'last_seen': None
        })
        
        # Historical data for trend analysis
        self.historical_windows = deque(maxlen=100)  # Keep last 100 windows
        self.kpi_history = deque(maxlen=1000)  # Keep last 1000 KPI records
        
        # Real-time counters
        self.current_window_start = None
        self.current_window_data = {
            'transactions': [],
            'users': set(),
            'products': set(),
            'categories': set(),
            'fraud_count': 0,
            'total_amount': 0.0
        }
    
    def add_transaction(self, transaction: Dict[str, Any], is_fraud: bool = False):
        """Add a transaction to the analytics engine."""
        timestamp = datetime.now(timezone.utc)
        
        # Add to transaction buffer
        self.transaction_buffer.append({
            'transaction': transaction,
            'timestamp': timestamp,
            'is_fraud': is_fraud
        })
        
        # Update current window data
        self._update_current_window(transaction, timestamp, is_fraud)
        
        # Update performance tracking
        self._update_performance_metrics(transaction, timestamp)
        
        # Check if we need to process a window
        if self._should_process_window(timestamp):
            self._process_window()
    
    def _update_current_window(self, transaction: Dict[str, Any], timestamp: datetime, is_fraud: bool):
        """Update current window data."""
        if self.current_window_start is None:
            self.current_window_start = timestamp
        
        # Check if we need to start a new window
        if (timestamp - self.current_window_start).total_seconds() >= self.window_size_seconds:
            self._process_window()
            self._start_new_window(timestamp)
        
        # Add to current window
        self.current_window_data['transactions'].append(transaction)
        self.current_window_data['users'].add(transaction.get('user_id', ''))
        self.current_window_data['products'].add(transaction.get('product_id', ''))
        self.current_window_data['categories'].add(transaction.get('product_category', ''))
        self.current_window_data['total_amount'] += transaction.get('amount', 0)
        
        if is_fraud:
            self.current_window_data['fraud_count'] += 1
    
    def _start_new_window(self, timestamp: datetime):
        """Start a new analytics window."""
        self.current_window_start = timestamp
        self.current_window_data = {
            'transactions': [],
            'users': set(),
            'products': set(),
            'categories': set(),
            'fraud_count': 0,
            'total_amount': 0.0
        }
    
    def _should_process_window(self, timestamp: datetime) -> bool:
        """Check if we should process the current window."""
        if self.current_window_start is None:
            return False
        
        return (timestamp - self.current_window_start).total_seconds() >= self.window_size_seconds
    
    def _process_window(self):
        """Process the current window and generate analytics."""
        if not self.current_window_data['transactions']:
            return
        
        window_end = datetime.now(timezone.utc)
        window_duration = (window_end - self.current_window_start).total_seconds()
        
        # Calculate window analytics
        analytics = self._calculate_window_analytics(
            self.current_window_start,
            window_end,
            window_duration
        )
        
        # Store historical data
        self.historical_windows.append(analytics)
        
        # Calculate and store KPIs
        kpis = self._calculate_kpi_metrics(analytics)
        self.kpi_history.append(kpis)
        
        # Log analytics
        self._log_analytics(analytics, kpis)
        
        # Start new window
        self._start_new_window(window_end)
    
    def _calculate_window_analytics(self, window_start: datetime, window_end: datetime, 
                                  window_duration: float) -> AnalyticsWindow:
        """Calculate comprehensive analytics for a time window."""
        transactions = self.current_window_data['transactions']
        total_transactions = len(transactions)
        total_sales = self.current_window_data['total_amount']
        unique_users = len(self.current_window_data['users'])
        unique_products = len(self.current_window_data['products'])
        unique_categories = len(self.current_window_data['categories'])
        fraud_count = self.current_window_data['fraud_count']
        
        # Calculate averages
        average_transaction_amount = total_sales / total_transactions if total_transactions > 0 else 0
        fraud_rate = fraud_count / total_transactions if total_transactions > 0 else 0
        
        # Calculate top categories
        category_sales = defaultdict(float)
        category_counts = defaultdict(int)
        for transaction in transactions:
            category = transaction.get('product_category', 'Unknown')
            amount = transaction.get('amount', 0)
            category_sales[category] += amount
            category_counts[category] += 1
        
        top_categories = [
            {'category': cat, 'sales': sales, 'count': category_counts[cat]}
            for cat, sales in sorted(category_sales.items(), key=lambda x: x[1], reverse=True)[:5]
        ]
        
        # Calculate top products
        product_sales = defaultdict(float)
        product_counts = defaultdict(int)
        for transaction in transactions:
            product_id = transaction.get('product_id', 'Unknown')
            product_name = transaction.get('product_name', 'Unknown')
            amount = transaction.get('amount', 0)
            product_sales[product_id] += amount
            product_counts[product_id] += 1
        
        top_products = [
            {'product_id': pid, 'product_name': transactions[0].get('product_name', 'Unknown'), 
             'sales': sales, 'count': product_counts[pid]}
            for pid, sales in sorted(product_sales.items(), key=lambda x: x[1], reverse=True)[:5]
        ]
        
        # Calculate payment method distribution
        payment_methods = defaultdict(int)
        for transaction in transactions:
            method = transaction.get('payment_method', 'Unknown')
            payment_methods[method] += 1
        
        # Calculate device type distribution
        device_types = defaultdict(int)
        for transaction in transactions:
            device = transaction.get('device_type', 'Unknown')
            device_types[device] += 1
        
        # Calculate geographic distribution
        countries = defaultdict(int)
        for transaction in transactions:
            location = transaction.get('location', {})
            country = location.get('country', 'Unknown')
            countries[country] += 1
        
        # Calculate additional metrics
        conversion_rate = 1.0  # Assuming all transactions are conversions for now
        revenue_per_user = total_sales / unique_users if unique_users > 0 else 0
        transactions_per_user = total_transactions / unique_users if unique_users > 0 else 0
        
        return AnalyticsWindow(
            window_start=window_start,
            window_end=window_end,
            window_duration_seconds=int(window_duration),
            total_transactions=total_transactions,
            total_sales=total_sales,
            average_transaction_amount=average_transaction_amount,
            unique_users=unique_users,
            unique_products=unique_products,
            unique_categories=unique_categories,
            top_categories=top_categories,
            top_products=top_products,
            payment_method_distribution=dict(payment_methods),
            device_type_distribution=dict(device_types),
            geographic_distribution=dict(countries),
            fraud_rate=fraud_rate,
            conversion_rate=conversion_rate,
            revenue_per_user=revenue_per_user,
            transactions_per_user=transactions_per_user
        )
    
    def _calculate_kpi_metrics(self, analytics: AnalyticsWindow) -> KPIMetrics:
        """Calculate KPI metrics for the analytics window."""
        timestamp = analytics.window_end
        
        # Basic metrics
        total_revenue = analytics.total_sales
        total_transactions = analytics.total_transactions
        average_order_value = analytics.average_transaction_amount
        fraud_rate = analytics.fraud_rate
        
        # Top performing category and product
        top_category = analytics.top_categories[0]['category'] if analytics.top_categories else 'N/A'
        top_product = analytics.top_products[0]['product_name'] if analytics.top_products else 'N/A'
        
        # Calculate growth rates
        revenue_growth_rate = self._calculate_growth_rate('revenue')
        customer_acquisition_rate = self._calculate_customer_acquisition_rate()
        
        # Calculate transaction velocity (transactions per minute)
        transaction_velocity = total_transactions / (analytics.window_duration_seconds / 60)
        
        return KPIMetrics(
            timestamp=timestamp,
            total_revenue=total_revenue,
            total_transactions=total_transactions,
            average_order_value=average_order_value,
            conversion_rate=analytics.conversion_rate,
            fraud_rate=fraud_rate,
            top_performing_category=top_category,
            top_performing_product=top_product,
            customer_acquisition_rate=customer_acquisition_rate,
            revenue_growth_rate=revenue_growth_rate,
            transaction_velocity=transaction_velocity
        )
    
    def _calculate_growth_rate(self, metric: str) -> float:
        """Calculate growth rate for a specific metric."""
        if len(self.kpi_history) < 2:
            return 0.0
        
        current_value = getattr(self.kpi_history[-1], metric, 0)
        previous_value = getattr(self.kpi_history[-2], metric, 0)
        
        if previous_value == 0:
            return 0.0
        
        return ((current_value - previous_value) / previous_value) * 100
    
    def _calculate_customer_acquisition_rate(self) -> float:
        """Calculate customer acquisition rate."""
        if len(self.historical_windows) < 2:
            return 0.0
        
        current_users = self.historical_windows[-1].unique_users
        previous_users = self.historical_windows[-2].unique_users
        
        return current_users - previous_users
    
    def _update_performance_metrics(self, transaction: Dict[str, Any], timestamp: datetime):
        """Update product and category performance metrics."""
        product_id = transaction.get('product_id', '')
        category = transaction.get('product_category', '')
        amount = transaction.get('amount', 0)
        user_id = transaction.get('user_id', '')
        
        # Update product performance
        if product_id:
            self.product_performance[product_id]['total_sales'] += amount
            self.product_performance[product_id]['transaction_count'] += 1
            self.product_performance[product_id]['unique_users'].add(user_id)
            self.product_performance[product_id]['last_seen'] = timestamp
        
        # Update category performance
        if category:
            self.category_performance[category]['total_sales'] += amount
            self.category_performance[category]['transaction_count'] += 1
            self.category_performance[category]['unique_users'].add(user_id)
            self.category_performance[category]['last_seen'] = timestamp
    
    def _log_analytics(self, analytics: AnalyticsWindow, kpis: KPIMetrics):
        """Log analytics results."""
        self.logger.info(f"""
        === ANALYTICS WINDOW ({analytics.window_start} - {analytics.window_end}) ===
        Total Transactions: {analytics.total_transactions}
        Total Sales: ${analytics.total_sales:.2f}
        Average Transaction: ${analytics.average_transaction_amount:.2f}
        Unique Users: {analytics.unique_users}
        Fraud Rate: {analytics.fraud_rate:.2%}
        Top Category: {analytics.top_categories[0]['category'] if analytics.top_categories else 'N/A'}
        Revenue Growth: {kpis.revenue_growth_rate:.2f}%
        Transaction Velocity: {kpis.transaction_velocity:.2f} tx/min
        """)
    
    def get_current_metrics(self) -> Dict[str, Any]:
        """Get current real-time metrics."""
        if not self.current_window_data['transactions']:
            return {}
        
        current_time = datetime.now(timezone.utc)
        window_duration = (current_time - self.current_window_start).total_seconds() if self.current_window_start else 0
        
        return {
            'current_window_duration': window_duration,
            'transactions_in_current_window': len(self.current_window_data['transactions']),
            'sales_in_current_window': self.current_window_data['total_amount'],
            'users_in_current_window': len(self.current_window_data['users']),
            'fraud_count_in_current_window': self.current_window_data['fraud_count'],
            'window_start': self.current_window_start.isoformat() if self.current_window_start else None
        }
    
    def get_historical_summary(self, hours: int = 24) -> Dict[str, Any]:
        """Get historical analytics summary for the specified time period."""
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours)
        
        # Filter windows within the time period
        recent_windows = [
            w for w in self.historical_windows 
            if w.window_start >= cutoff_time
        ]
        
        if not recent_windows:
            return {}
        
        # Calculate aggregated metrics
        total_revenue = sum(w.total_sales for w in recent_windows)
        total_transactions = sum(w.total_transactions for w in recent_windows)
        total_users = len(set().union(*(w.users for w in recent_windows)))
        avg_fraud_rate = statistics.mean(w.fraud_rate for w in recent_windows)
        
        return {
            'time_period_hours': hours,
            'total_revenue': total_revenue,
            'total_transactions': total_transactions,
            'total_unique_users': total_users,
            'average_fraud_rate': avg_fraud_rate,
            'windows_analyzed': len(recent_windows),
            'revenue_per_hour': total_revenue / hours,
            'transactions_per_hour': total_transactions / hours
        }
    
    def export_analytics_json(self) -> str:
        """Export current analytics data as JSON."""
        data = {
            'current_metrics': self.get_current_metrics(),
            'historical_summary_24h': self.get_historical_summary(24),
            'recent_windows': [asdict(w) for w in list(self.historical_windows)[-10:]],  # Last 10 windows
            'recent_kpis': [asdict(k) for k in list(self.kpi_history)[-10:]]  # Last 10 KPI records
        }
        
        return json.dumps(data, default=str, indent=2)
