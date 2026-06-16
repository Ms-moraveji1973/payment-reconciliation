import structlog
import logging
import sys

def setup_logger():
    structlog.configure(
        processors=[
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer()
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    logger_factory=structlog.PrintLoggerFactory()
    )
    return structlog.get_logger()

log = setup_logger()