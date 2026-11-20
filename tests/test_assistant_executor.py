"""The assistant's backtest path, on code written the way its prompt teaches."""
import asyncio
import tempfile
import unittest
from pathlib import Path

from zipfinam.assistant.agent import BacktestConfig
from zipfinam.assistant.data_manager import DataManager
from zipfinam.assistant.executor import BacktestExecutor, _parse_date_tz

from tests.test_backtest_pipeline import fake_finam

#: An SMA crossover in the prompt's own style: async history, polars columns, MarketOrder.
SMA_CROSSOVER = """
async def initialize(context):
    context.stocks = [await context.symbol("SBER@MISX"), await context.symbol("GAZP@MISX")]


async def handle_data(context, data):
    for stock in context.stocks:
        df = await data.history(assets=[stock], fields=["close"], bar_count=20)
        if len(df) < 20:
            continue
        closes = df["close"].to_numpy()
        target = 0.5 if closes[-5:].mean() > closes.mean() else 0.0
        await context.order_target_percent(asset=stock, target=target, style=MarketOrder())
"""


class AssistantExecutorTests(unittest.TestCase):

    def test_prompt_style_code_runs_against_a_benchmark(self):
        config = BacktestConfig(symbols=["SBER@MISX", "GAZP@MISX"], start_date="2025-02-03",
                                end_date="2025-03-28", capital=1_000_000,
                                benchmark="IMOEX@MISX")
        with tempfile.TemporaryDirectory() as tmp, fake_finam():
            manager = DataManager(assets_db_path=Path(tmp) / "assets.sqlite",
                                  bundle_storage_path=Path(tmp) / "data")
            result = asyncio.run(self._run(manager, config))

        self.assertFalse(result.errors, result.errors)
        self.assertGreater(result.total_return_pct, 0)

    @staticmethod
    async def _run(manager: DataManager, config: BacktestConfig):
        await manager.ensure_data(config.symbols + [config.benchmark],
                                  _parse_date_tz(config.start_date),
                                  _parse_date_tz(config.end_date))
        return await BacktestExecutor(manager).run(SMA_CROSSOVER, config)


if __name__ == "__main__":
    unittest.main()
