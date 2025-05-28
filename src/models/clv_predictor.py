import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import lightgbm as lgb
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

class CLVPredictor:
    def __init__(self):
        self.model = None
        self.scaler = StandardScaler()
        self.feature_columns = [
            'total_orders',
            'average_order_value',
            'days_since_first_order',
            'days_since_last_order',
            'abandoned_checkouts_count',
            'refunds_count',
            'refund_rate'
        ]

    def _prepare_features(self, customer_data: Dict) -> pd.DataFrame:
        """Prepare features for the model."""
        now = datetime.now()
        
        # Calculate temporal features
        first_order_date = pd.to_datetime(customer_data['first_order_date'])
        last_order_date = pd.to_datetime(customer_data['last_order_date'])
        
        features = {
            'total_orders': customer_data['total_orders'],
            'average_order_value': customer_data['average_order_value'],
            'days_since_first_order': (now - first_order_date).days if first_order_date else 0,
            'days_since_last_order': (now - last_order_date).days if last_order_date else 0,
            'abandoned_checkouts_count': customer_data['abandoned_checkouts_count'],
            'refunds_count': customer_data['refunds_count'],
            'refund_rate': customer_data['total_refunded'] / customer_data['total_spent'] if customer_data['total_spent'] > 0 else 0
        }
        
        return pd.DataFrame([features])

    def train(self, training_data: List[Dict], target_values: List[float]):
        """Train the CLV prediction model."""
        # Prepare features
        X = pd.DataFrame([self._prepare_features(d).iloc[0] for d in training_data])
        y = np.array(target_values)
        
        # Split data
        X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42)
        
        # Scale features
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_val_scaled = self.scaler.transform(X_val)
        
        # Create LightGBM datasets
        train_data = lgb.Dataset(X_train_scaled, label=y_train)
        val_data = lgb.Dataset(X_val_scaled, label=y_val)
        
        # Set parameters
        params = {
            'objective': 'regression',
            'metric': 'rmse',
            'num_leaves': 31,
            'learning_rate': 0.05,
            'feature_fraction': 0.9
        }
        
        # Train model
        self.model = lgb.train(
            params,
            train_data,
            valid_sets=[train_data, val_data],
            num_boost_round=100,
            early_stopping_rounds=10,
            verbose_eval=False
        )

    def predict(self, customer_data: Dict) -> Dict:
        """Predict future CLV for a customer."""
        if not self.model:
            raise ValueError("Model needs to be trained first")
        
        # Prepare features
        X = self._prepare_features(customer_data)
        X_scaled = self.scaler.transform(X)
        
        # Make prediction
        predicted_clv = self.model.predict(X_scaled)[0]
        
        # Calculate confidence score based on data quality
        confidence_score = self._calculate_confidence_score(customer_data)
        
        return {
            'predicted_clv': predicted_clv,
            'confidence_score': confidence_score,
            'prediction_date': datetime.now().isoformat(),
            'features_used': self.feature_columns
        }

    def _calculate_confidence_score(self, customer_data: Dict) -> float:
        """Calculate confidence score for the prediction."""
        # Factors that influence confidence:
        # 1. Number of orders
        # 2. Recency of last order
        # 3. Data completeness
        
        score = 1.0
        
        # Reduce confidence if few orders
        if customer_data['total_orders'] < 3:
            score *= 0.7
        
        # Reduce confidence if last order is too old (> 180 days)
        last_order_date = pd.to_datetime(customer_data['last_order_date'])
        if last_order_date and (datetime.now() - last_order_date).days > 180:
            score *= 0.8
        
        # Reduce confidence if missing important data
        if not customer_data['first_order_date'] or not customer_data['last_order_date']:
            score *= 0.6
        
        return round(score, 2) 