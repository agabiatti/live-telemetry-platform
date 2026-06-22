"""Manutenção manual do lake Silver: compaction + vacuum.

O sink do Silver é append-only (throughput): cada micro-batch vira 1 arquivo Parquet, então
o Delta acumula milhares de small files e a leitura do dashboard (`to_pyarrow_table` varre
todos) degrada. Este job — rodado FORA do hot path (`make compact`) — compacta os small files
em poucos arquivos grandes por partição (snapshot enxuto → read ~100x mais rápido) e faz vacuum
pra recuperar o disco dos arquivos tombstoned.

Seguro de rodar com o Silver no ar: compaction/vacuum são transações Delta ACID; o stream
continua escrevendo. Idempotente: rodar 2x não muda nada (já compactado é pulado).

Uso: python -m live_telemetry.silver.compact   (ou `make compact`)
"""

from __future__ import annotations

import os

from deltalake import DeltaTable

from live_telemetry.common.config import load_config
from live_telemetry.common.logging import get_logger

log = get_logger(__name__)

# Reter 0h no vacuum: como o lake é local/efêmero do desafio (não há leitores time-travel),
# removemos imediatamente os arquivos tombstoned pela compaction. enforce=False permite < 168h.
VACUUM_RETENTION_HOURS = 0


def _count_files(path: str) -> int:
    return sum(1 for r, _d, fs in os.walk(path) for f in fs if f.endswith(".parquet"))


def compact_table(path: str) -> None:
    """Compacta + vacuum uma tabela Delta. No-op se ela não existir ainda."""
    if not os.path.exists(os.path.join(path, "_delta_log")):
        log.info("compact_skip_absent", path=path)
        return

    before_disk = _count_files(path)
    dt = DeltaTable(path)
    before_snapshot = len(dt.files())

    dt.optimize.compact()
    dt.update_incremental()
    after_snapshot = len(dt.files())

    # Remove os small files tombstoned pela compaction (recupera disco).
    dt.vacuum(
        retention_hours=VACUUM_RETENTION_HOURS,
        enforce_retention_duration=False,
        dry_run=False,
    )
    after_disk = _count_files(path)

    log.info(
        "compact_done",
        path=path,
        snapshot_files=f"{before_snapshot}->{after_snapshot}",
        disk_files=f"{before_disk}->{after_disk}",
    )


def main() -> int:
    cfg = load_config()
    silver = cfg.paths.silver
    for table in ("session_window_qoe", "cdn_alerts"):
        compact_table(os.path.join(silver, table))
    log.info("compact_all_done", silver=silver)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
