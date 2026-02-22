import logging
import sys

def setup_logger(name):
    """
    Sets up a logger with a human-readable format.
    Output: [Time] [Level] [LoggerName] Message
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Check if handlers already exist to avoid duplicate logs
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        
        # Format: 2025-12-24 15:30:01 INFO [milvus_insert] Batch 10/100 completed
        formatter = logging.Formatter(
            '%(asctime)s %(levelname)s [%(name)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        # Prevent propagation to root logger to avoid double logging if root is configured
        logger.propagate = False
        
    return logger

