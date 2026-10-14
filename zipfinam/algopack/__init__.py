"""MOEX AlgoPack tables as point-in-time data sources.

Two ways to read one from a strategy::

    df = await data.history(assets=[sber], bar_count=20, data_source="algopack://eq/tradestats")

-- ziplime finds the ``algopack://`` scheme through the ``ziplime.address_schemes`` entry point, so
nothing has to be imported for it --

or, to choose the granularity, fields or window::

    from zipfinam.algopack import algopack_dataset

    context.flow = algopack_dataset(context, "tradestats", granularity="5min")
    df = await data.history(assets=[sber], bar_count=12, data_source=context.flow)

The key is read from ``ALGOPACK_API``.
"""
import datetime

from zipfinam.algopack.algopack_data_source import AlgoPackDataSource, is_address

__all__ = ["AlgoPackDataSource", "algopack_dataset", "install_address_scheme", "is_address",
           "mount_address"]


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


def mount_address(address: str, *, start_date: datetime.date, end_date: datetime.date,
                  trading_calendar, **_) -> AlgoPackDataSource:
    """Mount ``algopack://market/dataset[/granularity]`` for the running simulation.

    What ziplime calls for an ``algopack://`` address (see
    :mod:`ziplime.data.data_sources.address_schemes`). No network and no asset lookup: AlgoPack is
    read per instrument, so a mount knows nothing until a strategy says which instruments it is
    asking about. Arguments ziplime passes that a mount does not need are accepted and ignored.
    """
    return AlgoPackDataSource.mount(address, start_date=start_date, end_date=end_date,
                                    trading_calendar=trading_calendar)


def install_address_scheme() -> None:
    """Register the ``algopack://`` scheme with ziplime. Idempotent.

    ziplime 2.11 also finds it through the ``ziplime.address_schemes`` entry point; registering on
    import as well keeps a source checkout without installed metadata working. zipfinam 1.26.10
    did this by wrapping ``TradingAlgorithm._resolve_named_data_source``; that patch is gone.
    """
    from ziplime.data.data_sources.address_schemes import register_address_scheme

    register_address_scheme("algopack", mount_address)
