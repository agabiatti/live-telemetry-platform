"""Sink Delta do Silver — regressão do crash de coluna all-None.

Bug: 1ª janela do Silver fecha antes de qualquer marker SCTE chegar (consumer async) →
marker_id/break_type/scte_event_id vêm todos None → pyarrow infere tipo `null` → Delta
rejeita (`SchemaMismatchError: Invalid data type for Delta Lake: Null`) e derruba o stream.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pyarrow as pa
from deltalake import DeltaTable

from live_telemetry.silver.sink import _DeltaUpsertPartition

HINTS = {"marker_id": pa.string(), "break_type": pa.string(), "scte_event_id": pa.int64()}


def _row(session: str, marker: dict | None) -> dict:
    return {
        "session_id": session,
        "window_start": datetime(2026, 5, 20, 22, 14, tzinfo=timezone.utc),
        "cdn": "cdn-a",
        "events": 3,
        "marker_id": marker["marker_id"] if marker else None,
        "break_type": marker["break_type"] if marker else None,
        "scte_event_id": marker["scte_event_id"] if marker else None,
    }


def test_create_with_all_none_marker_columns(tmp_path) -> None:
    """Primeiro batch sem nenhum marker (3 colunas all-None) não pode quebrar o sink."""
    part = _DeltaUpsertPartition(str(tmp_path), ["session_id", "window_start"], HINTS)
    part.write_batch([_row("s1", None), _row("s2", None)])  # não lança

    dt = DeltaTable(str(tmp_path))
    assert pa.types.is_string(dt.schema().to_pyarrow().field("marker_id").type)
    assert pa.types.is_integer(dt.schema().to_pyarrow().field("scte_event_id").type)
    assert dt.to_pyarrow_table().num_rows == 2


def test_later_batch_with_marker_merges(tmp_path) -> None:
    """Tabela criada all-None; batch seguinte com marker real faz upsert sem mismatch de tipo."""
    part = _DeltaUpsertPartition(str(tmp_path), ["session_id", "window_start"], HINTS)
    part.write_batch([_row("s1", None)])
    part.write_batch([_row("s2", {"marker_id": "m9", "break_type": "commercial", "scte_event_id": 1003})])

    rows = {r["session_id"]: r for r in DeltaTable(str(tmp_path)).to_pyarrow_table().to_pylist()}
    assert rows["s1"]["marker_id"] is None
    assert rows["s2"]["marker_id"] == "m9"
    assert rows["s2"]["scte_event_id"] == 1003
