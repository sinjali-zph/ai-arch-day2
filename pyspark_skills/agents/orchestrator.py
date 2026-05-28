"""
Demographics Orchestrator

Reads a GitHub issue (title + body), decides which demographics operation
to run, then delegates to the appropriate subagent.

Model:  claude-opus-4-7  (adaptive thinking — needed to reason about intent)
Tools:  one per operation (detect / encrypt / remove / redact)

Usage:
    from pyspark_skills.agents import run_orchestrator

    result = run_orchestrator(
        issue_title="Encrypt demographics data in patient_pipeline.py",
        issue_body="Please hash first_name and last_name columns.",
        file_content=open("patient_pipeline.py").read(),
        file_path="patient_pipeline.py",
    )
    print(result["operation"])      # "encrypt"
    print(result["output"])         # modified source code (or detection report)
"""

import pathlib
import anthropic

from .subagents import detect_agent, encrypt_agent, remove_agent, redact_agent

# ---------------------------------------------------------------------------
# Client + shared context
# ---------------------------------------------------------------------------

_client = anthropic.Anthropic()
_SKILLS_MD = (pathlib.Path(__file__).parent.parent / "skills.md").read_text()

# ---------------------------------------------------------------------------
# Tool definitions — one per operation
# ---------------------------------------------------------------------------

_TOOLS: list[dict] = [
    {
        "name": "detect_demographics",
        "description": (
            "Scan a PySpark script and report which columns contain PII / demographic data. "
            "Use when the issue asks to detect, scan, audit, or identify PII columns."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "file_path": {
                    "type": "string",
                    "description": "Path to the target PySpark file.",
                },
            },
            "required": [],
            "additionalProperties": False,
        },
    },
    {
        "name": "encrypt_demographics",
        "description": (
            "Encrypt or SHA-256 hash PII columns in a PySpark script. "
            "Use when the issue asks to encrypt, hash, or pseudonymise demographics data."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "file_path": {"type": "string"},
                "columns": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Specific column names to encrypt. Omit to auto-detect all PII columns.",
                },
                "mode": {
                    "type": "string",
                    "enum": ["hash", "aes"],
                    "description": "hash = SHA-256 one-way (default). aes = AES-GCM reversible (Spark 3.3+).",
                },
            },
            "required": [],
            "additionalProperties": False,
        },
    },
    {
        "name": "remove_demographics",
        "description": (
            "Drop all PII / demographic columns from a PySpark script entirely. "
            "Use when the issue asks to remove, drop, or strip demographics."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "file_path": {"type": "string"},
                "columns": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Extra column names to drop in addition to auto-detected PII columns.",
                },
            },
            "required": [],
            "additionalProperties": False,
        },
    },
    {
        "name": "redact_demographics",
        "description": (
            "Replace PII column values with a static redaction placeholder. "
            "Use when the issue asks to redact or mask demographics data."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "file_path": {"type": "string"},
                "columns": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Specific columns to redact. Omit to redact all auto-detected PII columns.",
                },
                "redact_value": {
                    "type": "string",
                    "description": "Replacement string. Defaults to ***REDACTED***.",
                },
            },
            "required": [],
            "additionalProperties": False,
        },
    },
]

# ---------------------------------------------------------------------------
# Orchestrator system prompt
# ---------------------------------------------------------------------------

_SYSTEM = f"""You are a demographics-privacy orchestrator for PySpark pipelines.

You receive a GitHub issue and the content of a PySpark file.
Your job is to:
1. Read the issue title and body carefully.
2. Decide which SINGLE demographics operation the issue is requesting.
3. Call the matching tool with the correct parameters extracted from the issue.

## Skill reference
{_SKILLS_MD}

## Decision rules
- encrypt / hash / pseudonymise  →  encrypt_demographics
- remove / drop / strip          →  remove_demographics
- redact / mask                  →  redact_demographics
- detect / scan / audit          →  detect_demographics
- handle / process / sensitive   →  encrypt_demographics (default)

## Column extraction
- If the issue names specific columns (e.g. "first_name", "last_name"), pass them.
- If no columns are named, omit the `columns` parameter (subagent will auto-detect).

## Encryption mode
- Default to mode="hash" (SHA-256, one-way).
- Use mode="aes" ONLY if the issue explicitly requests reversible or AES encryption.

Call exactly ONE tool then stop.
"""

# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_orchestrator(
    issue_title: str,
    issue_body: str,
    file_content: str = "",
    file_path: str = "unknown.py",
) -> dict:
    """
    Parse a GitHub issue and apply the correct demographics operation.

    Args:
        issue_title:  GitHub issue title.
        issue_body:   GitHub issue body.
        file_content: Content of the target PySpark file (empty → subagent generates a stub).
        file_path:    Path/name of the file (used in subagent prompts).

    Returns:
        {
          "operation": str,          # detect | encrypt | remove | redact
          "tool_input": dict,        # parameters Claude chose
          "output": str | dict,      # modified code (str) or detection report (dict)
        }
    """
    user_message = (
        f"## Issue title\n{issue_title}\n\n"
        f"## Issue body\n{issue_body}\n\n"
        f"## File: {file_path}\n```python\n{file_content}\n```"
    )

    messages: list[dict] = [{"role": "user", "content": user_message}]

    # ── Agentic loop (runs at most twice: one tool call + one end_turn) ──────
    while True:
        response = _client.messages.create(
            model="claude-opus-4-7",
            max_tokens=1024,
            thinking={"type": "adaptive"},
            system=_SYSTEM,
            tools=_TOOLS,
            tool_choice={"type": "any"},   # must call exactly one tool
            messages=messages,
        )

        # Collect tool-use blocks
        tool_uses = [b for b in response.content if b.type == "tool_use"]

        if not tool_uses:
            # Claude returned end_turn without a tool call — shouldn't happen
            # given tool_choice="any", but handle gracefully
            return {
                "operation": "unknown",
                "tool_input": {},
                "output": "Orchestrator could not determine the requested operation.",
            }

        # Take the first (and only expected) tool call
        tool_block = tool_uses[0]
        operation = tool_block.name
        tool_input: dict = tool_block.input  # type: ignore[attr-defined]

        # ── Dispatch to the appropriate subagent ─────────────────────────────
        resolved_path = tool_input.get("file_path", file_path)
        cols = tool_input.get("columns") or None

        if operation == "detect_demographics":
            output = detect_agent(file_content=file_content, file_path=resolved_path)

        elif operation == "encrypt_demographics":
            output = encrypt_agent(
                file_content=file_content,
                file_path=resolved_path,
                columns=cols,
                encrypt_all_detected=(cols is None),
                mode=tool_input.get("mode", "hash"),
            )

        elif operation == "remove_demographics":
            output = remove_agent(
                file_content=file_content,
                file_path=resolved_path,
                columns=cols,
            )

        elif operation == "redact_demographics":
            output = redact_agent(
                file_content=file_content,
                file_path=resolved_path,
                columns=cols,
                redact_value=tool_input.get("redact_value", "***REDACTED***"),
            )

        else:
            output = f"Unknown operation: {operation}"

        return {
            "operation": operation.replace("_demographics", ""),
            "tool_input": tool_input,
            "output": output,
        }
