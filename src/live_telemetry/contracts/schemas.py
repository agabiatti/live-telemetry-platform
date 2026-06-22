"""Carregamento dos schemas Avro versionados de CONTRACTS/.

Mapa estável stream → schemas e subjects (TopicNameStrategy: `<topic>-value`).
A versão canônica de um payload é o schema-id no fio (registry), não um campo no corpo.
"""

from __future__ import annotations

import json
import os
from functools import lru_cache
from pathlib import Path

CONTRACTS_DIR = Path(os.environ.get("CONTRACTS_DIR", "CONTRACTS"))

# Schema "ativo" (writer/reader corrente) por stream. v2 do player é o reader atual.
ACTIVE_SCHEMA = {
    "player_events": "player_events.v2",
    "scte35_markers": "scte35_markers.v1",
    "content_metadata": "content_metadata.v1",
}

# Histórico de versões por subject (ordem de registro). Usado pelo bootstrap.
SCHEMA_HISTORY = {
    "player_events": ["player_events.v1", "player_events.v2"],
    "scte35_markers": ["scte35_markers.v1"],
    "content_metadata": ["content_metadata.v1"],
}


def schema_path(name: str) -> Path:
    return CONTRACTS_DIR / f"{name}.avsc"


@lru_cache(maxsize=None)
def load_schema_str(name: str) -> str:
    """String crua do .avsc (a serializar/registrar)."""
    return schema_path(name).read_text(encoding="utf-8")


def load_schema(name: str) -> dict:
    """Schema Avro parseado (dict)."""
    return json.loads(load_schema_str(name))


def subject_for(topic: str) -> str:
    """TopicNameStrategy."""
    return f"{topic}-value"
