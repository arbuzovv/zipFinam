"""What became of an order after the broker accepted it.

Finam answers ``POST /v1/accounts/{id}/orders`` as soon as it has *accepted* an
order and hands back its id; whether the order then executed is a separate
question, answered by ``GET /v1/accounts/{id}/orders/{order_id}``. Stopping at
the id reported an order the broker went on to reject as a successful tick, and
the next tick — still seeing no position — sent the same order again.

Pure functions (no ziplime, no network) so the worker and the tests share them.
"""
from typing import Any

# Finam OrderStatus values (finam_trade_api.order.model.OrderStatus).
FILLED_STATUSES = frozenset({"ORDER_STATUS_FILLED", "ORDER_STATUS_EXECUTED"})
REJECTED_STATUSES = frozenset({
    "ORDER_STATUS_REJECTED", "ORDER_STATUS_DENIED_BY_BROKER",
    "ORDER_STATUS_REJECTED_BY_EXCHANGE", "ORDER_STATUS_FAILED",
})
CANCELLED_STATUSES = frozenset({
    "ORDER_STATUS_CANCELED", "ORDER_STATUS_EXPIRED", "ORDER_STATUS_DONE_FOR_DAY",
    "ORDER_STATUS_REPLACED",
})
TERMINAL_STATUSES = FILLED_STATUSES | REJECTED_STATUSES | CANCELLED_STATUSES

# Outcomes that mean the strategy's decision never reached the market. A tick
# with one of these is a failure, not a success: nothing was bought or sold.
NOT_EXECUTED = frozenset({"rejected", "cancelled"})

OUTCOME_SEVERITY = {
    "filled": "info",
    "partially_filled": "warning",
    "pending": "warning",
    "unknown": "warning",
    "rejected": "error",
    "cancelled": "error",
}

OUTCOME_EVENT_TYPE = {
    "filled": "order_filled",
    "partially_filled": "order_partially_filled",
    "pending": "order_pending",
    "unknown": "order_status_unknown",
    "rejected": "order_rejected",
    "cancelled": "order_cancelled",
}


def decimal_value(value: Any) -> float | None:
    """A Finam decimal (``{"value": "141"}``, ``"141"`` or a number) as a float."""
    if isinstance(value, dict):
        value = value.get("value")
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def is_terminal(status: str | None) -> bool:
    return (status or "").upper() in TERMINAL_STATUSES


def order_outcome(status: str | None, executed_quantity: float | None) -> str:
    """Map a broker status (plus what executed) onto one of OUTCOME_SEVERITY's keys."""
    status = (status or "").upper()
    executed = executed_quantity or 0
    if not status:
        return "unknown"
    if status in FILLED_STATUSES:
        return "filled"
    if status in REJECTED_STATUSES:
        return "rejected"
    if status in CANCELLED_STATUSES:
        return "partially_filled" if executed else "cancelled"
    # Still working at the broker (NEW, PENDING_NEW, PARTIALLY_FILLED, WAIT, ...).
    return "partially_filled" if executed else "pending"


def describe_order(order: dict[str, Any]) -> str:
    """One human line for a log, an event message or a run error."""
    side = "BUY" if str(order.get("side", "")).upper().endswith("BUY") else "SELL"
    quantity = order.get("quantity")
    what = f"{side} {quantity:g} {order.get('symbol')}" if isinstance(quantity, (int, float)) \
        else f"{side} {order.get('symbol')}"
    order_id = order.get("exchange_order_id")
    status = order.get("status") or "no status"
    outcome = order.get("outcome")
    executed = order.get("executed_quantity")
    if outcome == "filled":
        return f"Order {order_id} filled: {what}"
    if outcome == "rejected":
        return f"Order {order_id} rejected by the broker ({status}): {what} not executed"
    if outcome == "cancelled":
        return f"Order {order_id} cancelled before execution ({status}): {what} not executed"
    if outcome == "partially_filled":
        return f"Order {order_id} partially filled ({status}): {executed or 0:g} of {what}"
    if outcome == "pending":
        return (f"Order {order_id} not executed yet ({status}): {what} is still working "
                "at the broker and may fill later")
    error = order.get("status_error")
    return f"Order {order_id} status unknown: {what}" + (f" ({error})" if error else "")
