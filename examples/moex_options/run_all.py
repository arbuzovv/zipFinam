"""Run the MOEX margined-option examples.

The report carries no return: the option prices are generated, and any figure about earnings would
be a statement about the generator rather than about the market. What it shows is what the run
did.
"""
import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from _harness import list_strategies, run_strategy, summarise  # noqa: E402

from ziplime.data.data_sources.options.synthetic import SYNTHETIC_DATA_WARNING  # noqa: E402
from ziplime.utils.logging_utils import configure_logging  # noqa: E402


async def main(only: list[str] | None = None, verbose: bool = False):
    strategies = list_strategies()
    if only:
        strategies = [s for s in strategies if any(k in s["name"] for k in only)]
    if not strategies:
        raise SystemExit("No strategy matched.")

    rows, failures = [], []
    for info in strategies:
        try:
            result, context = await run_strategy(info)
            rows.append(summarise(info, result, context))
        except Exception as error:
            failures.append((info["name"], f"{type(error).__name__}: {error}"))
            if verbose:
                import traceback
                traceback.print_exc()

    print()
    print("=" * 104)
    print("THE UNDERLYING IS REAL (SBER daily bars over gRPC). THE OPTION CHAIN IS SYNTHETIC.")
    print(SYNTHETIC_DATA_WARNING)
    print("=" * 104)
    header = (f"{'strategy':<24} {'window':<24} {'sess':>5} {'exp':>5} {'contr':>6} "
              f"{'trades':>7} {'lots':>6} {'peak margin':>12}")
    print(header)
    print("-" * len(header))
    for row in rows:
        print(f"{row['name']:<24} {row['window']:<24} {row['sessions']:>5} {row['expiries']:>5} "
              f"{row['contracts_listed']:>6} {row['option_trades']:>7} {row['lots']:>6} "
              f"{row['peak_margin']:>10,.0f}")
        for error in row["errors"]:
            print(f"    ! {error.message[:150]}")
    for name, error in failures:
        print(f"{name:<24} ERROR  {error[:110]}")
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--only", nargs="*")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    configure_logging()
    asyncio.run(main(only=args.only, verbose=args.verbose))
