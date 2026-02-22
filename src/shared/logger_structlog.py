import structlog
import sys
import logging

def setup_structlog():
    """
    Configures structlog to output JSON for production (non-interactive)
    and pretty console logs for local development (interactive).
    """
    
    # Shared processors for both modes
    processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
    ]

    # Detect if we are in an interactive terminal
    if sys.stdout.isatty():
        # Local development: Pretty colors and human readable
        processors.extend([
            structlog.dev.ConsoleRenderer()
        ])
    else:
        # Production/Kubernetes: JSON output for aggregators
        processors.extend([
            structlog.processors.JSONRenderer()
        ])

    structlog.configure(
        processors=processors,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
    return structlog.get_logger()
