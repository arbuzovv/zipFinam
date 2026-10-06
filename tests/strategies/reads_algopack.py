"""Reads the order-flow imbalance of SBER from AlgoPack by address, and records it."""


async def initialize(context):
    context.sber = await context.symbol("SBER@MISX")


async def handle_data(context, data):
    flow = await data.history(assets=[context.sber], fields=["disb"], bar_count=1,
                              data_source="algopack://eq/tradestats")
    context.record(disb=None if flow.is_empty() else flow["disb"][-1])
