"""MOEX AlgoPack tables as point-in-time data sources.

Two ways to read one from a strategy::

    import zipfinam  # adds the algopack:// scheme to data.history

    df = await data.history(assets=[sber], bar_count=20, data_source="algopack://eq/tradestats")

or, to choose the granularity, fields or window::

    from zipfinam.algopack import algopack_dataset

    context.flow = algopack_dataset(context, "tradestats", granularity="5min")
    df = await data.history(assets=[sber], bar_count=12, data_source=context.flow)

The key is read from ``ALGOPACK_API``.
"""
import datetime

from zipfinam.algopack.algopack_data_source import AlgoPackDataSource, is_address

__all__ = ["AlgoPackDataSource", "algopack_dataset", "install_address_scheme", "is_address"]


def algopack_dataset(context, dataset: str, market: str = "eq", granularity: str = "1d",
                     fields: list[str] | None = None,
                     start_date: datetime.date | None = None,
                     end_date: datetime.date | None = None,
                     name: str | None = None) -> AlgoPackDataSource:
    """Mount an AlgoPack table over the running simulation's window and calendar.

    Args:
        context: The ``context`` a strategy's ``initialize`` receives.
        dataset: ``tradestats``, ``orderstats``, ``obstats``, ``hi2``, ``alerts`` or ``futoi``.
        market: ``eq`` (equities), ``fo`` (futures) or ``fx``.
        granularity: ``1d`` for one row per Moscow session, or ``5min`` for the buckets exactly
            as the exchange publishes them.
        fields: Columns to keep besides ``date`` and ``sid``. All of them by default.
        start_date, end_date: Window to fetch. Defaults to the simulation's own.
        name: What to call the source. Defaults to its address.

    Returns:
        The mounted source. Nothing is fetched until a read names the instruments.
    """
    clock = context.clock
    return AlgoPackDataSource.mount(
        f"algopack://{market}/{dataset}", granularity=granularity, fields=fields, name=name,
        start_date=start_date or clock.start_session, end_date=end_date or clock.end_session,
        trading_calendar=clock.trading_calendar)


def install_address_scheme() -> None:
    """Teach ``data.history(data_source=...)`` the ``algopack://`` scheme.

    ziplime resolves a data source it does not know by name in
    ``TradingAlgorithm._resolve_named_data_source``, which understands only ``hf://``. This wraps
    it: an AlgoPack address is mounted here, anything else goes to ziplime unchanged. Idempotent.
    """
    from ziplime.trading.trading_algorithm import TradingAlgorithm

    original = TradingAlgorithm._resolve_named_data_source
    if getattr(original, "_zipfinam", False):
        return

    async def _resolve_named_data_source(self, name: str):
        if is_address(name):
            return AlgoPackDataSource.mount(
                name, start_date=self.clock.start_session, end_date=self.clock.end_session,
                trading_calendar=self.clock.trading_calendar)
        return await original(self, name)

    _resolve_named_data_source._zipfinam = True
    _resolve_named_data_source.__doc__ = original.__doc__
    TradingAlgorithm._resolve_named_data_source = _resolve_named_data_source
