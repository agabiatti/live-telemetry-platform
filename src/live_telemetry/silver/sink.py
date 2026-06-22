"""Sink Delta com MERGE upsert — idempotência de re-output do Silver.

Upsert por chave natural (session_id, window_start): recovery/late re-emit do Bytewax
reescrevem a mesma janela sem duplicar. NÃO é o dedupe de input (esse é por event_id).
"""

from __future__ import annotations

import os
from typing import Any

import pyarrow as pa
from bytewax.outputs import DynamicSink, StatelessSinkPartition
from deltalake import DeltaTable, write_deltalake

from live_telemetry.common.logging import get_logger

log = get_logger(__name__)


def _is_delta(path: str) -> bool:
    return os.path.exists(os.path.join(path, "_delta_log"))


def _coerce_null_columns(table: pa.Table, hints: dict[str, pa.DataType] | None) -> pa.Table:
    """Casta colunas que o pyarrow inferiu como tipo `null` para um tipo concreto.

    Quando um batch traz uma coluna inteira None (ex.: 1ª janela do Silver fecha antes de
    qualquer marker SCTE chegar → marker_id/break_type/scte_event_id todos None), pyarrow
    infere o tipo `null`, que o Delta Lake rejeita (`SchemaMismatchError: ... Null`) e quebra
    o stream. Coagimos para o tipo declarado (default string) para o schema ficar estável
    entre batches independentemente de quais campos estão preenchidos."""
    hints = hints or {}
    for i, field in enumerate(table.schema):
        if pa.types.is_null(field.type):
            target = hints.get(field.name, pa.string())
            table = table.set_column(i, field.name, table.column(i).cast(target))
    return table


class _DeltaUpsertPartition(StatelessSinkPartition):
    def __init__(
        self,
        path: str,
        key_cols: list[str],
        schema_hints: dict[str, pa.DataType] | None = None,
        partition_by: list[str] | None = None,
    ) -> None:
        self._path = path
        self._keys = key_cols
        self._hints = schema_hints
        self._partition_by = partition_by or []

    def write_batch(self, items: list[dict[str, Any]]) -> None:
        rows = [r for r in items if r is not None]
        if not rows:
            return
        table = _coerce_null_columns(pa.Table.from_pylist(rows), self._hints)
        if not _is_delta(self._path):
            os.makedirs(self._path, exist_ok=True)
            write_deltalake(self._path, table, partition_by=self._partition_by or None)
            log.info("silver_sink_created", path=self._path, rows=len(rows))
            return
        # Predicate inclui as colunas de partição → o MERGE faz partition pruning e só toca
        # os arquivos da(s) partição(ões) do batch, em vez de varrer a tabela toda (O(n²)).
        match_cols = list(self._partition_by) + [k for k in self._keys if k not in self._partition_by]
        predicate = " AND ".join(f"t.{k} = s.{k}" for k in match_cols)
        (
            DeltaTable(self._path)
            .merge(table, predicate=predicate, source_alias="s", target_alias="t")
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )

    def close(self) -> None:
        pass


class DeltaUpsertSink(DynamicSink):
    """Sink dinâmico: cada worker faz upsert idempotente por chave natural.

    `partition_by` particiona o Delta (e entra no predicate do MERGE) para pruning — sem isso
    o upsert varre a tabela inteira a cada batch e degrada O(n²) conforme o lake cresce."""

    def __init__(
        self,
        path: str,
        key_cols: list[str],
        schema_hints: dict[str, pa.DataType] | None = None,
        partition_by: list[str] | None = None,
    ) -> None:
        self._path = path
        self._keys = key_cols
        self._hints = schema_hints
        self._partition_by = partition_by or []

    def build(self, step_id: str, worker_index: int, worker_count: int) -> _DeltaUpsertPartition:
        return _DeltaUpsertPartition(self._path, self._keys, self._hints, self._partition_by)


class _DeltaAppendPartition(StatelessSinkPartition):
    def __init__(
        self,
        path: str,
        schema_hints: dict[str, pa.DataType] | None = None,
        partition_by: list[str] | None = None,
    ) -> None:
        self._path = path
        self._hints = schema_hints
        self._partition_by = partition_by or []

    def write_batch(self, items: list[dict[str, Any]]) -> None:
        rows = [r for r in items if r is not None]
        if not rows:
            return
        os.makedirs(self._path, exist_ok=True)
        table = _coerce_null_columns(pa.Table.from_pylist(rows), self._hints)
        # Append puro: O(1) por batch, sem read-modify-write nem replay do _delta_log (que o
        # MERGE per-batch fazia → custo de commit O(n²) conforme o log crescia). Idempotência
        # vai pro read (dedup por chave natural mantendo processed_at mais novo). Append gera
        # muitos small files → compactar fora do hot path com `silver.compact` (make compact).
        write_deltalake(
            self._path, table, mode="append", partition_by=self._partition_by or None
        )

    def close(self) -> None:
        pass


class DeltaAppendSink(DynamicSink):
    """Append-only — at-least-once. Para o QoE do Silver (throughput) e logs de alerta.

    Sem MERGE no hot path: cada batch é um append barato. Re-emits de recovery viram
    duplicatas, deduplicadas no read pela chave natural (mesma postura do Bronze/Gold)."""

    def __init__(
        self,
        path: str,
        schema_hints: dict[str, pa.DataType] | None = None,
        partition_by: list[str] | None = None,
    ) -> None:
        self._path = path
        self._hints = schema_hints
        self._partition_by = partition_by or []

    def build(self, step_id: str, worker_index: int, worker_count: int) -> _DeltaAppendPartition:
        return _DeltaAppendPartition(self._path, self._hints, self._partition_by)
