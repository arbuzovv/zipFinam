"""Capital allocation: a strategy trades its allocated capital, not the whole account.

A deployment freezes `total_cash` — the capital given to this strategy. The broker
account can hold far more (and other holdings), so seeding the ledger with the
whole account makes `order_target_percent(0.5)` mean half of the account. Instead
the strategy sees a sub-account:

* positions: only the instruments the strategy trades;
* cash: `capital - cost basis of those positions`;
* so portfolio value = capital + the unrealized P&L of its own positions.

The broker's own cash is deliberately not a cap: on a margin account it can be
negative, which turned the strategy's equity negative and made it sell short.
Funding is the broker's check to make ("No enough coverage").

Kept free of heavy imports so it can be tested without ziplime.

Known limit: realized P&L of closed round trips is not carried between ticks
(each tick restarts from `capital`); that needs the deployment state.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class Holding:
    symbol: str          # TICKER@MIC as the broker reports it
    quantity: float
    average_price: float


def strategy_holdings(holdings: Iterable[Holding], symbols: set[str] | None) -> list[Holding]:
    """The holdings that belong to the strategy (all of them when symbols is None)."""
    if not symbols:
        return list(holdings)
    wanted = {s.upper() for s in symbols}
    return [h for h in holdings if h.symbol.upper() in wanted]


def allocated_cash(*, capital: float, holdings: Iterable[Holding]) -> float:
    """Cash the strategy may use: its capital minus what its positions cost.

    Negative when its positions cost more than the allocation — the strategy
    then sells down toward its targets.
    """
    return capital - sum(h.quantity * h.average_price for h in holdings)


def round_to_lots(quantity: float, lot_size: float) -> float:
    """Shares rounded toward zero to whole lots (MOEX: GAZP trades in lots of 10)."""
    if lot_size <= 0:
        return quantity
    lots = int(abs(quantity) // lot_size)
    return (lots * lot_size) * (1 if quantity >= 0 else -1)
