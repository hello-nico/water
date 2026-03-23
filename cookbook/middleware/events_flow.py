"""
Events Flow Example: Real-Time Progress Streaming

This example demonstrates using an EventEmitter to stream real-time task
progress events during flow execution. A subscriber collects all events
and prints them after the flow completes.
"""

from water.core import Flow, create_task
from water.middleware.events import EventEmitter
from pydantic import BaseModel
from typing import Dict, Any, List
import asyncio


# Data schemas
class DocumentRequest(BaseModel):
    doc_id: str
    content: str


class ParsedDocument(BaseModel):
    doc_id: str
    word_count: int
    sentences: int


class AnalysisResult(BaseModel):
    doc_id: str
    word_count: int
    sentences: int
    avg_sentence_length: float
    summary: str


# Task: Parse document
def parse_document(params: Dict[str, Any], context) -> Dict[str, Any]:
    """Parse document into basic statistics."""
    data = params["input_data"]
    words = data["content"].split()
    sentences = [s.strip() for s in data["content"].split(".") if s.strip()]
    return {
        "doc_id": data["doc_id"],
        "word_count": len(words),
        "sentences": len(sentences),
    }


# Task: Analyze document
def analyze_document(params: Dict[str, Any], context) -> Dict[str, Any]:
    """Produce an analysis summary from parsed statistics."""
    data = params["input_data"]
    avg_len = data["word_count"] / max(data["sentences"], 1)
    return {
        "doc_id": data["doc_id"],
        "word_count": data["word_count"],
        "sentences": data["sentences"],
        "avg_sentence_length": round(avg_len, 2),
        "summary": f"Document {data['doc_id']}: {data['word_count']} words, "
                   f"{data['sentences']} sentences, avg {avg_len:.1f} words/sentence",
    }


# Create tasks
parse_task = create_task(
    id="parse",
    description="Parse the raw document",
    input_schema=DocumentRequest,
    output_schema=ParsedDocument,
    execute=parse_document,
)

analyze_task = create_task(
    id="analyze",
    description="Analyze parsed document statistics",
    input_schema=ParsedDocument,
    output_schema=AnalysisResult,
    execute=analyze_document,
)

# Build the flow with an EventEmitter
emitter = EventEmitter()

doc_flow = Flow(id="document_analysis", description="Document analysis with real-time events")
doc_flow.events = emitter
doc_flow.then(parse_task)\
    .then(analyze_task)\
    .register()


async def main():
    """Run the events streaming example."""

    document = {
        "doc_id": "DOC-001",
        "content": (
            "Water is a lightweight workflow framework. "
            "It supports sequential and parallel execution. "
            "Hooks and events provide real-time observability. "
            "Pydantic schemas ensure data integrity throughout the pipeline."
        ),
    }

    # Subscribe before running the flow
    subscription = await emitter.subscribe()
    collected_events: List[Dict[str, Any]] = []

    async def collect_events():
        async for event in subscription:
            collected_events.append(event.to_dict())

    # Run flow and event collector concurrently
    collector_task = asyncio.create_task(collect_events())

    try:
        result = await doc_flow.run(document)
        # Wait briefly for final events to propagate
        await asyncio.sleep(0.1)
        collector_task.cancel()

        print(f"Analysis: {result['summary']}")
        print(f"\nCollected {len(collected_events)} events:")
        for evt in collected_events:
            print(f"  [{evt['event_type']}] flow={evt['flow_id']} task={evt.get('task_id', '-')}")
        print("flow completed successfully!")
    except Exception as e:
        print(f"ERROR - {e}")


if __name__ == "__main__":
    asyncio.run(main())
