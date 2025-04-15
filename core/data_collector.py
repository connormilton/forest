"""
Data Collection Module
Handles market and account data collection from trading APIs
"""

import logging
import pandas as pd
from datetime import datetime, timezone, timedelta

logger = logging.getLogger("CollaborativeTrader")

class DataCollector:
    """Collects market and account data"""
    
    def __init__(self, ig_service, polygon_client):
        self.ig = ig_service
        self.polygon = polygon_client
    
    def get_account_data(self):
        """Get account information"""
        try:
            import os
            accounts = self.ig.fetch_accounts()
            if os.getenv("IG_ACCOUNT_ID"):
                account = accounts[accounts['accountId'] == os.getenv("IG_ACCOUNT_ID")]
            else:
                account = accounts.iloc[[0]]
            
            return account.iloc[0].to_dict()
        except Exception as e:
            logger.error(f"Error getting account: {e}")
            return {}
    
    def get_positions(self):
        """Get open positions"""
        try:
            return self.ig.fetch_open_positions()
        except Exception as e:
            logger.error(f"Error getting positions: {e}")
            return pd.DataFrame()
    
    def get_market_data(self, epic, timeframes=None):
        """Collect market data for an instrument"""
        if timeframes is None:
            timeframes = {
                "m15": {"timeframe": "15:minute", "lookback_days": 1},
                "h1": {"timeframe": "hour", "lookback_days": 2},
                "h4": {"timeframe": "4:hour", "lookback_days": 5}
            }
        
        # Convert IG epic to Polygon ticker
        ticker_map = {
            "CS.D.EURUSD.TODAY.IP": "C:EURUSD",
            "CS.D.USDJPY.TODAY.IP": "C:USDJPY",
            "CS.D.GBPUSD.TODAY.IP": "C:GBPUSD",
            "CS.D.AUDUSD.TODAY.IP": "C:AUDUSD",
            "CS.D.USDCAD.TODAY.IP": "C:USDCAD",
            "CS.D.USDCHF.TODAY.IP": "C:USDCHF",
            "CS.D.NZDUSD.TODAY.IP": "C:NZDUSD",
            "CS.D.EURJPY.TODAY.IP": "C:EURJPY",
            "CS.D.EURGBP.TODAY.IP": "C:EURGBP",
            "CS.D.GBPJPY.TODAY.IP": "C:GBPJPY",
            "CS.D.AUDJPY.TODAY.IP": "C:AUDJPY",
            "CS.D.AUDNZD.TODAY.IP": "C:AUDNZD"
        }
        
        ticker = ticker_map.get(epic)
        if not ticker:
            return {}
        
        results = {}
        
        try:
            for key, config in timeframes.items():
                timeframe = config["timeframe"]
                lookback_days = config["lookback_days"]
                
                # Parse timeframe
                if ":" in timeframe:
                    parts = timeframe.split(":")
                    multiplier = int(parts[0])
                    timespan = parts[1]
                else:
                    multiplier = 1
                    timespan = timeframe
                
                # Get data from Polygon
                end = datetime.now(timezone.utc)
                start = end - timedelta(days=lookback_days)
                
                aggs = self.polygon.get_aggs(
                    ticker=ticker,
                    multiplier=multiplier,
                    timespan=timespan,
                    from_=start.strftime("%Y-%m-%d"),
                    to=end.strftime("%Y-%m-%d"),
                    limit=100
                )
                
                if not aggs:
                    continue
                
                # Convert to standardized format
                data = [{
                    "timestamp": datetime.fromtimestamp(a.timestamp/1000, tz=timezone.utc).isoformat(),
                    "open": a.open,
                    "high": a.high,
                    "low": a.low,
                    "close": a.close,
                    "volume": a.volume
                } for a in aggs]
                
                results[key] = data
            
            # Add current price snapshot
            snapshot = self.get_price_snapshot(epic)
            if snapshot:
                results["current"] = snapshot
                
            return results
        except Exception as e:
            logger.error(f"Error collecting market data for {epic}: {e}")
            return {}
    
    def get_price_snapshot(self, epic):
        """Get current market price snapshot"""
        try:
            response = self.ig.fetch_market_by_epic(epic)
            if response and 'snapshot' in response:
                snapshot = response['snapshot']
                
                # Get raw values
                raw_bid = snapshot.get('bid')
                raw_offer = snapshot.get('offer')
                
                # Determine the divisor based on the currency pair
                divisor = 100.0 if "JPY" in epic else 10000.0
                
                # Convert points to decimal format
                bid = raw_bid / divisor if raw_bid is not None else None
                offer = raw_offer / divisor if raw_offer is not None else None
                
                return {
                    "bid": bid,
                    "offer": offer,
                    "epic": epic,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
            return None
        except Exception as e:
            logger.error(f"Error getting snapshot for {epic}: {e}")
            return None