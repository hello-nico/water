"""
Structured Output Flow Example: Sentiment Analysis with Pydantic Validation

This example demonstrates how to use create_structured_task to extract
validated Pydantic model instances from LLM responses.  The LLM is
instructed to respond in JSON matching the schema, and the output is
automatically parsed, validated, and retried on failure.

NOTE: Set ANTHROPIC_API_KEY or OPENAI_API_KEY to run this example.
"""

import asyncio
import json
import os

from pydantic import BaseModel

from water.core import Flow
from water.agents import create_structured_task
from water.agents.llm import OpenAIProvider, AnthropicProvider


def _get_provider(temperature=0.0):
    if os.environ.get("ANTHROPIC_API_KEY"):
        return AnthropicProvider(model="claude-haiku-4-5-20251001", temperature=temperature)
    return OpenAIProvider(model="gpt-4o-mini", temperature=temperature)


# ---------------------------------------------------------------------------
# Schema — the model the LLM must conform to
# ---------------------------------------------------------------------------

class SentimentResult(BaseModel):
    sentiment: str       # "positive", "negative", or "neutral"
    confidence: float    # 0.0 to 1.0
    reasoning: str       # brief explanation


# ---------------------------------------------------------------------------
# Build a structured task that extracts SentimentResult
# ---------------------------------------------------------------------------

async def main():
    print("=== Structured Output: Sentiment Analysis ===\n")

    provider = _get_provider(temperature=0.0)

    sentiment_task = create_structured_task(
        id="sentiment_analyzer",
        description="Analyzes sentiment of input text",
        prompt_template="Analyze the sentiment of the following text:\n\n{text}",
        system_prompt="You are a sentiment analysis expert. Respond ONLY with valid JSON matching this schema: {\"sentiment\": \"positive|negative|neutral\", \"confidence\": 0.0-1.0, \"reasoning\": \"brief explanation\"}",
        provider_instance=provider,
        model_cls=SentimentResult,
        max_retries=3,
    )

    # Run in a flow
    class TextInput(BaseModel):
        text: str

    flow = Flow(id="sentiment_flow", description="Structured sentiment analysis")
    flow.then(sentiment_task).register()

    result = await flow.run({"text": "I absolutely love this product! Best purchase ever."})

    # The result is a validated SentimentResult dict
    print(f"Sentiment:  {result['sentiment']}")
    print(f"Confidence: {result['confidence']}")
    print(f"Reasoning:  {result['reasoning']}")

    # You can also reconstruct the Pydantic model from the result
    validated = SentimentResult.model_validate(result)
    print(f"\nValidated model: {validated}")
    print("\nDone!")


if __name__ == "__main__":
    asyncio.run(main())
