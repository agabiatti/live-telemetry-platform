"""Deduplicação idempotente por event_id, bounded por event-time.

Trata duplicatas do broker (at-least-once, ~1%). É best-effort no Silver: o set é mantido
por sessão (chave do stateful_map) e podado por horizonte de event-time para não crescer
sem limite. Dup patológica muito tardia (fora do horizonte) escapa aqui e é pega no dedupe
global do Gold (ver DESIGN §5).

NÃO confundir com idempotência de sink: dedupe filtra re-input; o sink Delta+MERGE absorve
re-output de recovery/late re-emit.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any, Optional

# Estado por sessão: {event_id: event_time}
DedupeState = dict[str, Any]


def dedupe_step(
    state: Optional[DedupeState], event: dict[str, Any], horizon_s: int
) -> tuple[DedupeState, tuple[dict[str, Any], bool]]:
    """Mapper para stateful_map. Retorna (novo_estado, (event, is_dup)).

    is_dup=True quando o event_id já foi visto dentro do horizonte → será filtrado downstream.
    """
    if state is None:
        state = {}

    event_id = event["event_id"]
    event_time = event["event_time"]
    is_dup = event_id in state

    if not is_dup:
        state[event_id] = event_time

    # Poda: descarta ids mais antigos que (max_event_time - horizonte).
    if state:
        cutoff = max(state.values()) - timedelta(seconds=horizon_s)
        state = {eid: ts for eid, ts in state.items() if ts >= cutoff}

    return state, (event, is_dup)
