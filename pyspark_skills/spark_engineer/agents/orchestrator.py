"""
Spark Engineer Orchestrator

Reads a free-text request (or GitHub issue), decides which Spark operation
to perform, then delegates to the appropriate subagent.

Model:  claude-opus-4-7  (adaptive thinking — needed to reason about intent)
Tools:  one per operation (optimize / write / debug / stream)

Usage:
    from pyspark_skills.spark_engineer.agents import run_orchestrator

    result = run_orchestrator(
        request_title="Our join is taking 45 min — fix the skew",
        request_body="The groupBy on user_id is extremely slow.  File: etl.py",
        file_content=open("etl.py").read(),
        file_path="etl.py",
    )
    print(result["operation"])   # "optimize"
    print(result["output"])      # fixed source code
"""

import anthropic

from .subagents import optimize_agent, write_agent, debug_agent, stream_agent

# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

_client = anthropic.Anthropic()

# ---------------------------------------------------------------------------
# Tool definitions — one per operation
# ---------------------------------------------------------------------------

_TOOLS: list[dict] = [
    {
        "name": "optimize_pipeline",
        "description": (
            "Optimise an existing PySpark script for production performance. "
            "Use when the request asks to speed up, tune, optimise, fix partitions, "
            "reduce shuffle, fix skew, or improve join performance."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "file_path": {
                    "type": "string",
                    "description": "Path to the target PySpark file.",
                },
                "hint": {
                    "type": "string",
                    "description": "Specific concern extracted from the request (e.g. 'skewed groupBy on user_id').",
                },
            },
            "required": [],
            "additionalProperties": False,
        },
    },
    {
        "name": "write_pipeline",
        "description": (
            "Generate a brand-new production-grade PySpark ETL pipeline from requirements. "
            "Use when the request asks to create, build, write, or generate a new Spark job."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "requirements": {
                    "type": "string",
                    "description": "Full requirements for the pipeline extracted from the request.",
                },
                "file_path": {
                    "type": "string",
                    "description": "Desired output filename for the new pipeline.",
                },
                "source_format": {
                    "type": "string",
                    "description": "Input file format: parquet, csv, json, delta, etc. Default parquet.",
                },
                "sink_format": {
                    "type": "string",
                    "description": "Output file format: parquet, csv, delta, etc. Default parquet.",
                },
            },
            "required": ["requirements"],
            "additionalProperties": False,
        },
    },
    {
        "name": "debug_performance",
        "description": (
            "Identify and fix bugs or performance issues in an existing PySpark script. "
            "Use when the request describes a crash, OOM, slowness, incorrect results, "
            "or asks to audit / review existing code."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "file_path": {
                    "type": "string",
                    "description": "Path to the target PySpark file.",
                },
                "symptom": {
                    "type": "string",
                    "description": "The observed problem extracted from the request.",
                },
            },
            "required": [],
            "additionalProperties": False,
        },
    },
    {
        "name": "implement_streaming",
        "description": (
            "Implement a Spark Structured Streaming pipeline. "
            "Use when the request asks for real-time processing, streaming, Kafka, "
            "event-time windows, or continuous pipelines."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "requirements": {
                    "type": "string",
                    "description": "Full streaming requirements extracted from the request.",
                },
                "file_path": {
                    "type": "string",
                    "description": "Desired output filename.",
                },
                "source_type": {
                    "type": "string",
                    "description": "Streaming source type: kafka, socket, rate, file. Default kafka.",
                },
                "sink_type": {
                    "type": "string",
                    "description": "Sink type: parquet, console, kafka, memory. Default parquet.",
                },
                "watermark_delay": {
                    "type": "string",
                    "description": "Late-data watermark delay, e.g. '10 minutes'. Default '10 minutes'.",
                },
            },
            "required": ["requirements"],
            "additionalProperties": False,
        },
    },
]

# ---------------------------------------------------------------------------
# Orchestrator system prompt
# ---------------------------------------------------------------------------

_SYSTEM = """You are a Spark Engineering orchestrator.

You receive a request (title + body) and optionally the content of a PySpark file.
Your job is to:
1. Read the request carefully.
2. Decide which SINGLE Spark operation the request needs.
3. Call the matching tool with parameters extracted from the request.

## Decision rules
- speed up / tune / optimise / fix skew / fix join / partition  →  optimize_pipeline
- create / build / write / generate a new pipeline              →  write_pipeline
- crash / OOM / slow / incorrect / audit / review               →  debug_performance
- streaming / real-time / Kafka / event-time / window           →  implement_streaming

## Parameter extraction
- Extract the specific concern or symptom from the request body.
- If a filename is mentioned (e.g. "fix etl.py"), pass it as file_path.
- For write/stream operations, extract requirements verbatim from the request body.
- For source/sink formats, default to parquet unless the request states otherwise.

Call exactly ONE tool then stop.
"""

# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_orchestrator(
    request_title: str,
    request_body: str,
    file_content: str = "",
    file_path: str = "unknown.py",
) -> dict:
    """
    Parse a request and dispatch to the correct Spark Engineering subagent.

    Args:
        request_title: Short description of the task (e.g. GitHub issue title).
        request_body:  Full description of requirements or problem.
        file_content:  Content of the target PySpark file (empty for write/stream).
        file_path:     Path/name of the file.

    Returns:
        {
          "operation": str,       # optimize | write | debug | stream
          "tool_input": dict,     # parameters Claude chose
          "output": str,          # complete Python source code
        }
    """
    user_message = (
        f"## Request title\n{request_title}\n\n"
        f"## Request body\n{request_body}\n\n"
    )
    if file_content:
        user_message += f"## File: {file_path}\n```python\n{file_content}\n```"

    messages: list[dict] = [{"role": "user", "content": user_message}]

    # ── Agentic loop (one tool call + end_turn) ──────────────────────────────
    while True:
        response = _client.messages.create(
            model="claude-opus-4-7",
            max_tokens=1024,
            thinking={"type": "adaptive"},
            system=_SYSTEM,
            tools=_TOOLS,
            tool_choice={"type": "any"},
            messages=messages,
        )

        tool_uses = [b for b in response.content if b.type == "tool_use"]

        if not tool_uses:
            return {
                "operation": "unknown",
                "tool_input": {},
                "output": "Orchestrator could not determine the requested operation.",
            }

        tool_block = tool_uses[0]
        operation: str = tool_block.name
        tool_input: dict = tool_block.input  # type: ignore[attr-defined]

        resolved_path = tool_input.get("file_path", file_path)
        hint = tool_input.get("hint", "")
        symptom = tool_input.get("symptom", "")
        requirements = tool_input.get("requirements", request_body)

        if operation == "optimize_pipeline":
            output = optimize_agent(
                file_content=file_content,
                file_path=resolved_path,
                hint=hint,
            )

        elif operation == "write_pipeline":
            output = write_agent(
                requirements=requirements,
                file_path=tool_input.get("file_path", "pipeline.py"),
                source_format=tool_input.get("source_format", "parquet"),
                sink_format=tool_input.get("sink_format", "parquet"),
            )

        elif operation == "debug_performance":
            output = debug_agent(
                file_content=file_content,
                file_path=resolved_path,
                symptom=symptom,
            )

        elif operation == "implement_streaming":
            output = stream_agent(
                requirements=requirements,
                file_path=tool_input.get("file_path", "streaming_pipeline.py"),
                source_type=tool_input.get("source_type", "kafka"),
                sink_type=tool_input.get("sink_type", "parquet"),
                watermark_delay=tool_input.get("watermark_delay", "10 minutes"),
            )

        else:
            output = f"Unknown operation: {operation}"

        op_label = {
            "optimize_pipeline": "optimize",
            "write_pipeline": "write",
            "debug_performance": "debug",
            "implement_streaming": "stream",
        }.get(operation, operation)

        return {
            "operation": op_label,
            "tool_input": tool_input,
            "output": output,
        }
