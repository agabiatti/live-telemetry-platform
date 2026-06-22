"""Configuração central, lida de variáveis de ambiente (ver env.example).

Fonte única de parâmetros operacionais compartilhados entre as camadas. Mantida sem
dependências de domínio de propósito — qualquer módulo pode importar sem ciclo.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(key: str, default: str) -> str:
    return os.environ.get(key, default)


def _env_int(key: str, default: int) -> int:
    return int(os.environ.get(key, default))


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap: str = field(default_factory=lambda: _env("KAFKA_BOOTSTRAP", "localhost:9092"))
    schema_registry_url: str = field(
        default_factory=lambda: _env("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    )
    topic_player_events: str = field(default_factory=lambda: _env("TOPIC_PLAYER_EVENTS", "player_events"))
    topic_scte35: str = field(default_factory=lambda: _env("TOPIC_SCTE35", "scte35_markers"))
    topic_content: str = field(default_factory=lambda: _env("TOPIC_CONTENT", "content_metadata"))
    topic_ad_decisions: str = field(default_factory=lambda: _env("TOPIC_AD_DECISIONS", "ad_decisions"))


@dataclass(frozen=True)
class PathsConfig:
    data_dir: str = field(default_factory=lambda: _env("DATA_DIR", "data"))
    bronze: str = field(default_factory=lambda: _env("BRONZE_PATH", "data/bronze"))
    silver: str = field(default_factory=lambda: _env("SILVER_PATH", "data/silver"))
    gold: str = field(default_factory=lambda: _env("GOLD_PATH", "data/gold"))
    raw: str = field(default_factory=lambda: os.path.join(_env("DATA_DIR", "data"), "raw"))


@dataclass(frozen=True)
class StreamingConfig:
    """Parâmetros event-time do Silver. Defaults derivados das injeções do gerador
    (out-of-order 15s + clock skew 5s → watermark_delay ~25s). Ver DESIGN §5."""

    watermark_delay_s: int = field(default_factory=lambda: _env_int("WATERMARK_DELAY_S", 25))
    allowed_lateness_s: int = field(default_factory=lambda: _env_int("ALLOWED_LATENESS_S", 60))
    # Espera em SYSTEM-time do EventClock do Bytewax antes de fechar a janela. Interage com
    # REPLAY_SPEED (a 60x, 1 event-min ≈ 1s real). Default pequeno para o demo fechar rápido.
    system_wait_s: int = field(default_factory=lambda: _env_int("SILVER_SYSTEM_WAIT_S", 10))
    session_idle_timeout_s: int = field(default_factory=lambda: _env_int("SESSION_IDLE_TIMEOUT_S", 120))
    dedupe_horizon_s: int = field(default_factory=lambda: _env_int("DEDUPE_HORIZON_S", 180))
    window_size_s: int = field(default_factory=lambda: _env_int("WINDOW_SIZE_S", 60))


@dataclass(frozen=True)
class ReplayConfig:
    speed: int = field(default_factory=lambda: _env_int("REPLAY_SPEED", 60))


@dataclass(frozen=True)
class Config:
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    paths: PathsConfig = field(default_factory=PathsConfig)
    streaming: StreamingConfig = field(default_factory=StreamingConfig)
    replay: ReplayConfig = field(default_factory=ReplayConfig)
    slo_config: str = field(default_factory=lambda: _env("SLO_CONFIG", "OBSERVABILITY/slos.yaml"))
    log_level: str = field(default_factory=lambda: _env("LOG_LEVEL", "INFO"))


def load_config() -> Config:
    """Instancia a config a partir do ambiente atual."""
    return Config()
