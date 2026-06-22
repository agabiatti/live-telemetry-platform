"""Logging estruturado (structlog) — JSON em produção, console legível em dev.

Logs estruturados são requisito de avaliação ("Código e engenharia"). Todas as camadas
emitem via `get_logger(__name__)` para correlacionar eventos por serviço/campo.
"""

from __future__ import annotations

import logging
import os
import sys

import structlog


def configure_logging(level: str | None = None) -> None:
    log_level = (level or os.environ.get("LOG_LEVEL", "INFO")).upper()
    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=log_level)

    dev_mode = os.environ.get("LOG_DEV", "").lower() in {"1", "true", "yes"}
    renderer = structlog.dev.ConsoleRenderer() if dev_mode else structlog.processors.JSONRenderer()

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(getattr(logging, log_level, logging.INFO)),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)
