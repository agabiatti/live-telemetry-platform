"""Wrapper fino sobre o Schema Registry (compat BACKWARD).

Encapsula registro de schema e set de política de compatibilidade. Mantido pequeno
para ser fácil de testar/mockar e defender numa revisão.
"""

from __future__ import annotations

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient

from live_telemetry.common.logging import get_logger

log = get_logger(__name__)


class ContractRegistry:
    """Cliente de contratos: registra schemas e fixa compat BACKWARD por subject."""

    def __init__(self, url: str) -> None:
        self._client = SchemaRegistryClient({"url": url})

    @property
    def client(self) -> SchemaRegistryClient:
        return self._client

    def set_backward(self, subject: str) -> None:
        """BACKWARD: consumidor com schema novo lê dados escritos com schema antigo.
        Escolhido para evolução consumer-first; força que campos novos tenham default."""
        self._client.set_compatibility(subject_name=subject, level="BACKWARD")
        log.info("compat_set", subject=subject, level="BACKWARD")

    def register(self, subject: str, schema_str: str) -> int:
        schema_id = self._client.register_schema(subject, Schema(schema_str, schema_type="AVRO"))
        log.info("schema_registered", subject=subject, schema_id=schema_id)
        return schema_id
