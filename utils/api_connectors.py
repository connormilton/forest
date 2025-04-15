"""
API Connection Utilities
Handles connections to trading platforms and data providers
"""

import os
import logging
from trading_ig import IGService
from polygon import RESTClient

logger = logging.getLogger("CollaborativeTrader")

def get_ig_service():
    """Connect to IG API and work with whatever account is active"""
    try:
        # Get desired account ID
        desired_account = os.getenv("IG_ACCOUNT_ID", "INRKZ")
        
        # Basic connection to IG API
        ig = IGService(
            username=os.getenv("IG_USERNAME"),
            password=os.getenv("IG_PASSWORD"),
            api_key=os.getenv("IG_API_KEY"),
            acc_type=os.getenv("IG_ACC_TYPE", "DEMO")
        )
        
        # Create session
        ig.create_session()
        logger.info("IG API connected successfully")
        
        # Check active account
        try:
            accounts = ig.fetch_accounts()
            logger.info(f"Found {len(accounts)} accounts")
            
            # Display all available accounts
            available_accounts = []
            for _, account in accounts.iterrows():
                account_id = account['accountId']
                account_type = account['accountType']
                account_balance = account['balance']
                account_currency = account['currency']
                
                available_accounts.append(account_id)
                logger.info(f"Account: {account_id} ({account_type}) - Balance: {account_balance} {account_currency}")
            
            # Check active account
            active_account = accounts.iloc[0]['accountId']
            logger.info(f"Active account: {active_account}")
            
            # Provide warning if not using desired account
            if active_account != desired_account:
                warning_message = f"WARNING: Using account {active_account} instead of desired account {desired_account}"
                logger.warning(f"{'*' * 20}")
                logger.warning(warning_message)
                logger.warning(f"IG API is not connecting to the requested account despite multiple attempts")
                logger.warning(f"Proceeding with available account {active_account}")
                logger.warning(f"{'*' * 20}")
                
                # Print to console as well for visibility
                print(f"\n{'*' * 60}")
                print(f"WARNING: USING ACCOUNT {active_account} INSTEAD OF {desired_account}")
                print(f"{'*' * 60}\n")
            else:
                logger.info(f"Successfully connected to desired account {desired_account}")
                
            # Display balance of active account
            active_balance = accounts[accounts['accountId'] == active_account].iloc[0]['balance']
            active_currency = accounts[accounts['accountId'] == active_account].iloc[0]['currency']
            logger.info(f"Active account balance: {active_balance} {active_currency}")
            
            # Check if balance is sufficient for trading
            if active_balance <= 0:
                logger.warning(f"Account {active_account} has insufficient balance: {active_balance} {active_currency}")
                print(f"\nWARNING: ACCOUNT BALANCE IS {active_balance} {active_currency}")
                print(f"The system may not be able to execute trades with this balance\n")
                
        except Exception as e:
            logger.error(f"Error checking account details: {e}")
        
        return ig
    
    except Exception as e:
        logger.error(f"IG connection error: {e}")
        return None

def get_polygon_client():
    """Get Polygon API client"""
    return RESTClient(os.getenv("POLYGON_API_KEY"))

def execute_trade(ig_service, trade):
    """Execute a new trade on IG platform"""
    try:
        logger.info(f"Executing {trade.get('direction')} {trade.get('epic')} | Size: {trade.get('size')}")
        
        # Check if we have account info before attempting trade
        try:
            accounts = ig_service.fetch_accounts()
            active_account = accounts.iloc[0]['accountId']
            active_balance = accounts.iloc[0]['balance']
            active_currency = accounts.iloc[0]['currency']
            
            if active_balance <= 0:
                logger.warning(f"Attempting to trade with account {active_account} that has balance: {active_balance} {active_currency}")
        except Exception as check_error:
            logger.warning(f"Could not verify account balance before trade: {check_error}")
        
        # Directly based on working code from grasshooper.py
        response = ig_service.create_open_position(
            epic=trade["epic"],
            direction=trade["direction"],
            size=float(trade["size"]),
            order_type="MARKET",
            currency_code=os.getenv("ACCOUNT_CURRENCY", "GBP"),
            expiry="DFB",
            force_open=True,
            guaranteed_stop=False,
            stop_level=float(trade["initial_stop_loss"]) if "initial_stop_loss" in trade else None,
            limit_level=float(trade["take_profit_levels"][0]) if "take_profit_levels" in trade and trade["take_profit_levels"] else None
        )
        
        logger.info(f"Trade response: {response}")
        
        # Prepare trade log data
        from datetime import datetime, timezone
        
        trade_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "epic": trade["epic"],
            "direction": trade["direction"],
            "size": trade["size"],
            "entry_price": trade.get("entry_price"),
            "stop_loss": trade.get("initial_stop_loss"),
            "take_profit": trade.get("take_profit_levels")[0] if trade.get("take_profit_levels") else None,
            "risk_percent": trade.get("risk_percent"),
            "risk_reward": trade.get("risk_reward"),
            "pattern": trade.get("pattern"),
            "stop_management": trade.get("stop_management", []),
            "outcome": "EXECUTED" if response.get("dealStatus") == "ACCEPTED" else "FAILED",
            "deal_id": response.get("dealId"),
            "reason": response.get("reason", "")
        }
        
        return True, trade_data
    except Exception as e:
        logger.error(f"Trade execution error: {e}")
        return False, {"outcome": "ERROR", "reason": str(e)}

def close_position(ig_service, position_action, positions):
    """Close an existing position"""
    try:
        deal_id = position_action.get("dealId")
        epic = position_action.get("epic")
        
        logger.info(f"Closing position {deal_id} | {epic}")
        
        # Find position details
        position = positions[positions["dealId"] == deal_id]
        
        if position.empty:
            logger.warning(f"Position not found for close: {deal_id}")
            return False, {"outcome": "FAILED", "reason": "Position not found"}
            
        # Get position details
        direction = position.iloc[0].get("direction")
        size = position.iloc[0].get("size")
        
        # Execute close
        close_direction = "SELL" if direction == "BUY" else "BUY"
        
        response = ig_service.close_open_position(
            deal_id=deal_id,
            direction=close_direction,
            size=float(size),
            order_type="MARKET"
        )
        
        # Log the close
        from datetime import datetime, timezone
        
        close_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "epic": epic,
            "direction": "CLOSE",
            "outcome": "CLOSED" if response.get("dealStatus") == "ACCEPTED" else "FAILED",
            "deal_id": deal_id,
            "reason": position_action.get("reason", "")
        }
        
        return True, close_data
    except Exception as e:
        logger.error(f"Close position error: {e}")
        return False, {"outcome": "ERROR", "reason": str(e)}

def update_stop_loss(ig_service, position_action):
    """Update stop loss for an existing position"""
    try:
        deal_id = position_action.get("dealId")
        epic = position_action.get("epic")
        new_level = position_action.get("new_level")
        
        logger.info(f"Updating stop for {deal_id} to {new_level}")
        
        response = ig_service.update_open_position(
            deal_id=deal_id,
            stop_level=float(new_level)
        )
        
        # Log the update
        from datetime import datetime, timezone
        
        update_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "epic": epic,
            "action_type": "UPDATE_STOP",
            "deal_id": deal_id,
            "new_level": new_level,
            "outcome": "UPDATED" if response.get("dealStatus") == "ACCEPTED" else "FAILED",
            "reason": position_action.get("reason", "")
        }
        
        return True, update_data
    except Exception as e:
        logger.error(f"Update stop loss error: {e}")
        return False, {"outcome": "ERROR", "reason": str(e)}