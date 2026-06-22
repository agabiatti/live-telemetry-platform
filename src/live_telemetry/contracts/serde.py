"""Serializadores/deserializadores Avro ligados ao Schema Registry.

O producer usa um serializer por schema ativo (v2 para player). O schema-id é embutido
no fio pelo wire-format do Confluent; o deserializer resolve o writer-schema pelo id e
faz schema resolution contra o reader-schema fornecido (ou o do writer, se omitido).
"""

from __future__ import annotations

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer


def make_serializer(
    sr_client: SchemaRegistryClient, schema_str: str, *, auto_register: bool = True
) -> AvroSerializer:
    return AvroSerializer(
        sr_client,
        schema_str,
        conf={"auto.register.schemas": auto_register},
    )


def make_deserializer(
    sr_client: SchemaRegistryClient, reader_schema_str: str | None = None
) -> AvroDeserializer:
    """reader_schema_str=None → usa o writer schema (resolvido pelo id no fio)."""
    return AvroDeserializer(sr_client, reader_schema_str)
