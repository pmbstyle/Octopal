from __future__ import annotations

import logging
import sys
from contextvars import ContextVar
from pathlib import Path
from typing import Any

import structlog
from structlog.types import EventDict, Processor

# Context variable to hold the correlation ID for the current task chain.
correlation_id_var: ContextVar[str | None] = ContextVar("correlation_id", default=None)


def add_correlation_id(_: logging.Logger, __: str, event_dict: EventDict) -> EventDict:
    """A structlog processor to add the correlation ID from the context variable."""
    if correlation_id := correlation_id_var.get():
        event_dict["correlation_id"] = correlation_id
    return event_dict


def configure_logging(log_level: str, log_dir: Path, debug_prompts: bool) -> None:
    """
    Configures logging for the entire application using structlog.
    Sets up both a human-readable console logger and a JSON file logger.
    """
    log_path = log_dir / "broodmind.log"

    # These are the processors that will be applied to all log records.
    shared_processors: list[Processor] = [
        # Add shared context automatically
        structlog.contextvars.merge_contextvars,
        # Add our custom correlation ID
        add_correlation_id,
        # Add log level and logger name
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        # Add a timestamp
        structlog.processors.TimeStamper(fmt="iso"),
        # Apply standard string formatting to the message
        structlog.stdlib.PositionalArgumentsFormatter(),
    ]

    # Configure the standard logging library to be a bridge for structlog.
    # All loggers will now pass their records to structlog for processing.
    logging.basicConfig(level=log_level.upper(), stream=sys.stdout, format="%(message)s")

    # Configure structlog itself.
    structlog.configure(
        processors=[
            *shared_processors,
            # This processor must be last to perform the final rendering.
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # --- Handlers ---
    # We will have two handlers: one for the console and one for the file.

    # 1. Console Handler (for development)
    # This handler uses a ConsoleRenderer for pretty, colored, human-readable output.
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level.upper())
    
    console_formatter = structlog.stdlib.ProcessorFormatter(
        # The foreign_pre_chain is for logs coming from standard logging.
        foreign_pre_chain=shared_processors,
        processor=structlog.dev.ConsoleRenderer(colors=True, pad_level=False),
    )
    console_handler.setFormatter(console_formatter)

    # 2. File Handler (for production/auditing)
    # This handler uses a JSONRenderer to write logs in a machine-readable format.
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(log_level.upper())
    
    file_formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processor=structlog.processors.JSONRenderer(),
    )
    file_handler.setFormatter(file_formatter)

    # --- Root Logger Configuration ---
    # Get the root logger and remove any existing handlers.
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(log_level.upper())
    
    # Add our new handlers.
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)

    # Suppress verbose logging from other libraries by default.
    # They will still be captured and processed by our handlers if their level is WARNING or higher.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    
    # Suppress LiteLLM's verbose INFO logs including "Provider List" messages
    # We target both lowercase and mixed-case names as used internally by LiteLLM
    for name in ["litellm", "LiteLLM", "LiteLLM Router", "LiteLLM Proxy", "litellm.utils"]:
        logging.getLogger(name).setLevel(logging.WARNING)
        
    try:
        import litellm
        litellm.set_verbose = False
        litellm.suppress_debug_info = True
        litellm.turn_off_message_logging = True
    except ImportError:
        pass

    # Also suppress any LiteLLM sub-loggers that might be created
    for name in logging.root.manager.loggerDict:
        if name.lower().startswith("litellm"):
            logging.getLogger(name).setLevel(logging.WARNING)

    # Special handling for our own debug flags
    if not debug_prompts:
        # If debug_prompts is off, ensure provider logs are not at DEBUG
        logging.getLogger("broodmind.providers.litellm_provider").setLevel(logging.INFO)
    
    logger = structlog.get_logger("logging_config")
    logger.info(
        "Logging configured", 
        log_level=log_level, 
        log_path=str(log_path),
        debug_prompts=debug_prompts
    )
