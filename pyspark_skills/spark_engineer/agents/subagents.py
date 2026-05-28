"""
Specialized subagents for Spark Engineer operations.

Each function is a focused Claude API call that writes, optimizes, debugs,
or streams PySpark code.  The orchestrator selects the right one; subagents
never call each other.

Models:
  - claude-haiku-4-5 : fast, deterministic code generation (subagents)
"""

import anthropic

_client = anthropic.Anthropic()

_SYSTEM = (
    "You are a senior Apache Spark engineer. "
    "You receive PySpark code or requirements and return COMPLETE, production-ready PySpark source. "
    "Follow every rule given. Output ONLY Python source code — no markdown fences, no explanations."
)

# Spark best-practice reminders injected into every subagent prompt (cached).
_SPARK_RULES = """
## Spark best-practice rules (MUST follow)
- Use DataFrame API over RDD for structured data.
- Always define explicit StructType schemas for production pipelines.
- Enable AQE: spark.sql.adaptive.enabled = true.
- Set spark.sql.shuffle.partitions to a value appropriate for the data volume (default 200 is often wrong).
- Use broadcast() hint for dimension tables < 200 MB.
- Handle skew with salting (SALT_BUCKETS = 50 pattern).
- Cache only when a DataFrame is reused multiple times; call .count() to materialise; call .unpersist() when done.
- Never use collect() on large DataFrames.
- Verify partition count with df.rdd.getNumPartitions() before writing.
- No UDFs when equivalent built-in functions exist.
- Coalesce small-file outputs before writing.
"""


def _call(user_prompt: str) -> str:
    """Single-turn Haiku call with Spark rules cached in the system prompt."""
    response = _client.messages.create(
        model="claude-haiku-4-5",
        max_tokens=8192,
        system=[
            {
                "type": "text",
                "text": _SYSTEM + "\n\n" + _SPARK_RULES,
                "cache_control": {"type": "ephemeral"},
            }
        ],
        messages=[{"role": "user", "content": user_prompt}],
    )
    return next(b.text for b in response.content if b.type == "text")


# ---------------------------------------------------------------------------
# Subagent 1 — Optimize
# ---------------------------------------------------------------------------

def optimize_agent(
    file_content: str,
    file_path: str = "unknown.py",
    hint: str = "",
) -> str:
    """
    Analyse an existing PySpark script and apply performance optimisations.

    Applies: broadcast joins, partition tuning, skew salting, correct caching,
    AQE config, and small-file coalescing where appropriate.

    Returns the complete optimised Python source as a string.
    """
    hint_line = f"Specific concern from user: {hint}" if hint else ""
    prompt = f"""
File: {file_path}

```python
{file_content}
```

{hint_line}

Task: optimise this PySpark pipeline for production performance.

Rules:
1. Add AQE config if SparkSession is created here:
   .config("spark.sql.adaptive.enabled", "true")
   .config("spark.sql.shuffle.partitions", "400")  # adjust to data volume
2. Wrap any small dimension DataFrame in broadcast() at join sites.
3. Fix any incorrect cache pattern (add .count() after .cache(); add .unpersist()).
4. Add salting if a join key is visibly skewed (use SALT_BUCKETS = 50).
5. Add df.rdd.getNumPartitions() print before each .write call.
6. Replace UDFs with equivalent built-in F.* functions where possible.
7. Keep all logic and variable names unchanged unless fixing a performance issue.

Return the COMPLETE optimised Python source code only.
"""
    return _call(prompt)


# ---------------------------------------------------------------------------
# Subagent 2 — Write
# ---------------------------------------------------------------------------

def write_agent(
    requirements: str,
    file_path: str = "pipeline.py",
    source_format: str = "parquet",
    sink_format: str = "parquet",
) -> str:
    """
    Generate a new production-grade PySpark ETL pipeline from requirements.

    Args:
        requirements:  Free-text description of transformations, schema, and logic.
        file_path:     Desired output filename (used in docstring only).
        source_format: Input file format (parquet, csv, json, delta, etc.).
        sink_format:   Output file format.

    Returns the complete Python source as a string.
    """
    prompt = f"""
Task: write a complete, production-grade PySpark ETL pipeline.

Requirements:
{requirements}

Source format : {source_format}
Sink format   : {sink_format}
Output file   : {file_path}

Structure the script with these sections (in order):
1. Imports (pyspark.sql, pyspark.sql.functions as F, pyspark.sql.types)
2. SparkSession with AQE enabled and shuffle partitions configured
3. Explicit StructType schema definition
4. Data read with the explicit schema
5. Transformations (filter, join, aggregate, enrich)
6. Partition count verification print
7. Write to sink

Apply every Spark best-practice rule you know.
Return ONLY the complete Python source code.
"""
    return _call(prompt)


# ---------------------------------------------------------------------------
# Subagent 3 — Debug
# ---------------------------------------------------------------------------

def debug_agent(
    file_content: str,
    file_path: str = "unknown.py",
    symptom: str = "",
) -> str:
    """
    Identify and fix performance or correctness issues in an existing PySpark script.

    Args:
        file_content: Current source code.
        file_path:    File name (for context).
        symptom:      Description of observed problem (slow shuffle, OOM, skew, etc.).

    Returns the complete fixed Python source as a string, with inline comments
    explaining each fix prefixed with # FIX:.
    """
    symptom_line = f"Observed symptom: {symptom}" if symptom else "No specific symptom reported — do a full audit."
    prompt = f"""
File: {file_path}

```python
{file_content}
```

{symptom_line}

Task: identify all performance and correctness bugs and return a fixed version.

For each fix:
- Add a short inline comment prefixed with "# FIX:" explaining what was wrong.

Common issues to check:
- collect() on large DataFrames → use write or aggregation instead
- Missing broadcast() on small dimension joins
- Incorrect or missing .cache()/.unpersist() pairing
- Default shuffle partitions (200) on large data
- UDFs instead of built-in functions
- Schema inference instead of explicit StructType
- Uncoalesced small-file writes
- Data skew at join/groupBy keys

Return the COMPLETE fixed Python source code only.
"""
    return _call(prompt)


# ---------------------------------------------------------------------------
# Subagent 4 — Stream
# ---------------------------------------------------------------------------

def stream_agent(
    requirements: str,
    file_path: str = "streaming_pipeline.py",
    source_type: str = "kafka",
    sink_type: str = "parquet",
    watermark_delay: str = "10 minutes",
) -> str:
    """
    Generate a production-grade Spark Structured Streaming pipeline.

    Args:
        requirements:    Free-text description of the streaming logic and schema.
        file_path:       Desired output filename (for docstring).
        source_type:     Streaming source (kafka, socket, rate, file).
        sink_type:       Sink type (parquet, console, kafka, memory, foreach).
        watermark_delay: Late-data tolerance for event-time watermark.

    Returns the complete Python source as a string.
    """
    prompt = f"""
Task: write a complete Spark Structured Streaming pipeline.

Requirements:
{requirements}

Source type    : {source_type}
Sink type      : {sink_type}
Watermark delay: {watermark_delay}
Output file    : {file_path}

Structure the script with these sections:
1. Imports
2. SparkSession with streaming config
3. Explicit schema definition
4. readStream source configuration
5. Watermark definition (event-time column + delay)
6. Stateful transformations (windowed aggregations or flatMapGroupsWithState)
7. writeStream sink with checkpointLocation
8. awaitTermination

Rules:
- Always define a watermark before stateful aggregations.
- Set checkpointLocation to a durable path.
- Use outputMode "append" for windowed aggregations, "update" for stateful ops.
- For Kafka sources: parse the value column as JSON using from_json with explicit schema.
- Emit progress logs via spark.streams.active for observability.

Return ONLY the complete Python source code.
"""
    return _call(prompt)
