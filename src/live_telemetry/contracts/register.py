"""Bootstrap dos contratos no Schema Registry.

Para cada stream: registra a v1 (cria o subject), fixa compat BACKWARD e então
registra as versões seguintes (a v2 do player é validada contra BACKWARD).

Uso: python -m live_telemetry.contracts.register
"""

from __future__ import annotations

import sys

from live_telemetry.common.config import load_config
from live_telemetry.common.logging import configure_logging, get_logger
from live_telemetry.contracts.registry import ContractRegistry
from live_telemetry.contracts.schemas import SCHEMA_HISTORY, load_schema_str, subject_for

log = get_logger(__name__)


def bootstrap() -> None:
    cfg = load_config()
    reg = ContractRegistry(cfg.kafka.schema_registry_url)

    topics = {
        "player_events": cfg.kafka.topic_player_events,
        "scte35_markers": cfg.kafka.topic_scte35,
        "content_metadata": cfg.kafka.topic_content,
    }

    for stream, topic in topics.items():
        subject = subject_for(topic)
        versions = SCHEMA_HISTORY[stream]
        # v1 primeiro (cria o subject), depois fixa BACKWARD, depois evoluções.
        reg.register(subject, load_schema_str(versions[0]))
        reg.set_backward(subject)
        for name in versions[1:]:
            reg.register(subject, load_schema_str(name))
        log.info("subject_bootstrapped", subject=subject, versions=versions)


def main() -> int:
    configure_logging()
    try:
        bootstrap()
    except Exception as exc:  # noqa: BLE001 — boundary: log e falha explícita
        log.error("bootstrap_failed", error=str(exc))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
