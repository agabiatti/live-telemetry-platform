"""Detector de degradação de CDN — multi-burn-rate, dinâmico por CDN.

Mecanismo (ver DESIGN §6):
- burn_rate(janela) = fração_ruim(janela) / budget   (budget = alvo do SLO)
- multi-window AND: tier dispara só se janela longa E curta cruzam o threshold
- multi-tier: fast (page) e slow (ticket)
- gate de amostra mínima (n_min) + histerese (fire/clear consecutivos)
- avaliado POR CDN — cdn-b não é hardcoded; qualquer CDN que cruzar dispara.

Métrica = rebuffering ratio (fração ruim = rebuffer_ms / (rebuffer_ms + watch_ms)). Escolhida
em vez de error_rate porque a degradação injetada é primariamente de buffering (~9-16x; o burst
leva o rebuffering a ~8% vs SLO 1% → burn ~8), enquanto a fração de erros por minuto é modesta
(~2x, pois erro é terminal/esparso). error_rate fica como sinal alternativo.

`total`/`bad` definem a fração (em ms); `samples` (nº de eventos/janelas) alimenta o gate n_min,
separado da magnitude em ms. A janela longa do slow (30 min) é a memória do estado por CDN.

Calibração: thresholds (fast=4, slow=2) calibrados à severidade injetada. Em produção derivariam
do SLO real + latência de detecção desejada (tabela tipo Google SRE). O mecanismo é o mesmo.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional


@dataclass(frozen=True)
class BurnRateConfig:
    budget: float = 0.01            # alvo do SLO de rebuffering ratio (1%)
    fast_long_min: int = 5
    fast_short_min: int = 1
    fast_threshold: float = 4.0
    slow_long_min: int = 30
    slow_short_min: int = 5
    slow_threshold: float = 2.0
    n_min: int = 50                 # amostras mínimas (eventos) na janela longa para alertar
    fire_consecutive: int = 2
    clear_consecutive: int = 3


@dataclass
class CdnBurnState:
    # minuto_epoch (int) -> (total, bad, samples)
    minutes: dict[int, tuple[float, float, int]] = field(default_factory=dict)
    firing: bool = False
    tier: Optional[str] = None
    fire_count: int = 0
    clear_count: int = 0


def _minute_key(ts: datetime) -> int:
    return int(ts.timestamp() // 60)


def _window_sums(state: CdnBurnState, latest: int, window_min: int) -> tuple[float, float, int]:
    lo = latest - window_min + 1
    total = bad = 0.0
    samples = 0
    for minute, (t, b, s) in state.minutes.items():
        if minute >= lo:
            total += t
            bad += b
            samples += s
    return total, bad, samples


def _burn(state: CdnBurnState, latest: int, window_min: int, cfg: BurnRateConfig) -> float:
    total, bad, _ = _window_sums(state, latest, window_min)
    if total <= 0:
        return 0.0
    return (bad / total) / cfg.budget


def _evaluate(state: CdnBurnState, latest: int, cfg: BurnRateConfig) -> tuple[bool, Optional[str]]:
    _, _, fast_samples = _window_sums(state, latest, cfg.fast_long_min)
    _, _, slow_samples = _window_sums(state, latest, cfg.slow_long_min)

    fast = (
        fast_samples >= cfg.n_min
        and _burn(state, latest, cfg.fast_long_min, cfg) >= cfg.fast_threshold
        and _burn(state, latest, cfg.fast_short_min, cfg) >= cfg.fast_threshold
    )
    if fast:
        return True, "page"

    slow = (
        slow_samples >= cfg.n_min
        and _burn(state, latest, cfg.slow_long_min, cfg) >= cfg.slow_threshold
        and _burn(state, latest, cfg.slow_short_min, cfg) >= cfg.slow_threshold
    )
    if slow:
        return True, "ticket"
    return False, None


def step(
    state: Optional[CdnBurnState],
    cdn: str,
    window_start: datetime,
    total: float,
    bad: float,
    samples: int,
    cfg: BurnRateConfig,
) -> tuple[CdnBurnState, Optional[dict[str, Any]]]:
    """Ingere um agregado (cdn, janela 1min) e retorna (estado, transição_ou_None).

    `total`/`bad` definem a fração ruim; `samples` alimenta o gate n_min. Transição só na
    mudança de estado (FIRING/RESOLVED), com histerese.
    """
    if state is None:
        state = CdnBurnState()

    minute = _minute_key(window_start)
    t, b, s = state.minutes.get(minute, (0.0, 0.0, 0))
    state.minutes[minute] = (t + total, b + bad, s + samples)

    latest = max(state.minutes)
    cutoff = latest - cfg.slow_long_min  # poda além da janela longa
    state.minutes = {m: v for m, v in state.minutes.items() if m > cutoff}

    raw_active, tier = _evaluate(state, latest, cfg)
    transition: Optional[dict[str, Any]] = None

    if raw_active and not state.firing:
        state.fire_count += 1
        state.clear_count = 0
        if state.fire_count >= cfg.fire_consecutive:
            state.firing = True
            state.tier = tier
            transition = {"cdn": cdn, "state": "FIRING", "tier": tier, "minute": minute}
    elif not raw_active and state.firing:
        state.clear_count += 1
        state.fire_count = 0
        if state.clear_count >= cfg.clear_consecutive:
            state.firing = False
            transition = {"cdn": cdn, "state": "RESOLVED", "tier": state.tier, "minute": minute}
            state.tier = None
    else:
        if raw_active:
            state.tier = tier
            state.clear_count = 0
        else:
            state.fire_count = 0

    return state, transition
