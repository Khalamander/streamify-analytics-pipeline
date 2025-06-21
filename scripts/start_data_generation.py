#!/usr/bin/env python3
"""
Data Generation Starter Script
Week 11: Complete Orchestration Pipeline and Testing

This script starts the data generation process for testing and demonstration.
"""

import sys
import os
import time
import signal
import logging
from datetime import datetime, timezone

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.producer.producer import TransactionDataGenerator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataGenerationManager:
    """Manages data generation process with proper signal handling."""
    
    def __init__(self):
        self.generator = None
        self.running = False
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False
        
        if self.generator:
            self.generator.close()
        
        sys.exit(0)
    
    def start_generation(self, duration_minutes: int = 60, batch_size: int = 50):
        """Start data generation process."""
        try:
            logger.info("Starting data generation process...")
            
            # Initialize generator
            self.generator = TransactionDataGenerator()
            self.running = True
            
            # Start generation
            self.generator.run_continuous_generation(
                batch_size=batch_size,
                batch_interval=2.0,
                duration_minutes=duration_minutes
            )
            
        except Exception as e:
            logger.error(f"Error in data generation: {e}")
            raise
        finally:
            if self.generator:
                self.generator.close()
            logger.info("Data generation process stopped")

def main():
    """Main function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Start Streamify data generation')
    parser.add_argument('--duration', type=int, default=60, 
                       help='Duration in minutes (default: 60)')
    parser.add_argument('--batch-size', type=int, default=50,
                       help='Batch size for transactions (default: 50)')
    
    args = parser.parse_args()
    
    try:
        manager = DataGenerationManager()
        manager.start_generation(
            duration_minutes=args.duration,
            batch_size=args.batch_size
        )
    except KeyboardInterrupt:
        logger.info("Data generation stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
