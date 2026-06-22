"""Entrypoint do serviço Silver (always-on). Roda o dataflow Bytewax em single-worker.

Em produção: `python -m bytewax.run` multi-worker + RecoveryConfig (snapshots). Aqui usamos
run_main (single-worker) para simplicidade do demo; recovery é discutido no DESIGN §5.
"""

from __future__ import annotations

from bytewax.testing import run_main

from live_telemetry.common.config import load_config
from live_telemetry.common.logging import configure_logging, get_logger
from live_telemetry.silver.dataflow import build_flow

log = get_logger(__name__)


def main() -> None:
    configure_logging()
    cfg = load_config()
    log.info("silver_boot", bootstrap=cfg.kafka.bootstrap, silver_path=cfg.paths.silver,
             system_wait_s=cfg.streaming.system_wait_s)
    flow = build_flow(cfg)
    run_main(flow)


if __name__ == "__main__":
    main()
