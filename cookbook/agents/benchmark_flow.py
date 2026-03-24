"""
Benchmark Flow — Run tool-use and instruction benchmarks across real LLM providers.

Demonstrates using BenchmarkRunner to compare two providers and print a
leaderboard.  Uses OpenAIProvider with real API calls.

Usage:
    python cookbook/agents/benchmark_flow.py
"""

import asyncio
import json
import tempfile
import os

from water.agents.llm import OpenAIProvider, AnthropicProvider
from water.bench import BenchmarkRunner, ToolUseBenchmark, InstructionBenchmark


def _get_provider(temperature=0.0):
    if os.environ.get("ANTHROPIC_API_KEY"):
        return AnthropicProvider(model="claude-haiku-4-5-20251001", temperature=temperature)
    return OpenAIProvider(model="gpt-4o-mini", temperature=temperature)


async def main():
    # --- Set up two real providers with different temperatures ---
    good_provider = _get_provider(temperature=0.0)
    bad_provider = _get_provider(temperature=1.0)  # high temp = worse instruction following

    # --- Run benchmarks ---
    runner = BenchmarkRunner(
        providers={
            "good_model": good_provider,
            "bad_model": bad_provider,
        },
        benchmarks=[
            ToolUseBenchmark(),
            InstructionBenchmark(),
        ],
    )

    print("Running benchmarks...")
    report = await runner.run()

    # --- Print leaderboard ---
    print("\n=== Leaderboard ===\n")
    print(report.leaderboard())

    # --- Print summary ---
    print("\n=== Summary ===\n")
    summary = report.summary()
    print(json.dumps(summary, indent=2))

    # --- Export to JSON ---
    export_path = os.path.join(tempfile.gettempdir(), "benchmark_results.json")
    report.export(export_path)
    print(f"\nResults exported to: {export_path}")


if __name__ == "__main__":
    asyncio.run(main())
