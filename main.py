"""
Collaborative LLM Forex Trading System
Main entry point
"""

import os
import logging
from dotenv import load_dotenv
from utils.api_connectors import get_ig_service, get_polygon_client
from core.system_controller import SystemController

# Setup
load_dotenv()
os.makedirs("data", exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("data/trading_system.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("CollaborativeTrader")

def main():
    logger.info("Initializing Collaborative LLM Forex Trading System")
    
    # Connect to trading APIs
    ig_service = get_ig_service()
    polygon_client = get_polygon_client()
    
    if not ig_service:
        logger.error("Failed to connect to IG API. Exiting.")
        return
    
    # Initialize and run the system
    system = SystemController(ig_service, polygon_client)
    system.run()

if __name__ == "__main__":
    main()