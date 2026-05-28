"""
Specialized subagents for demographics operations.

Each function is a focused Claude API call that modifies or analyses
PySpark code using DemographicsHandler.  The orchestrator selects and
calls the right one; subagents never call each other.

Models:
  - claude-haiku-4-5  : fast, deterministic code generation (subagents)
"""

import pathlib
import anthropic

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_client = anthropic.Anthropic()

# Skills.md is loaded once and cached on every API call (prompt caching).
_SKILLS_MD = pathlib.Path(__file__).parent.parent / "skills.md"

_HANDLER_IMPORT = "from pyspark_skills import DemographicsHandler"

_SKILL_CONTEXT = _SKILLS_MD.read_text()

_SYSTEM = (
    "You are a PySpark code-modification assistant. "
    "You receive an existing PySpark script and return the COMPLETE modified script "
    "with DemographicsHandler applied exactly as instructed. "
    "Output ONLY the Python source code — no markdown fences, no explanations."
)


def _call(user_prompt: str) -> str:
    """Single-turn Haiku call with the skills doc cached in the system prompt."""
    response = _client.messages.create(
        model="claude-haiku-4-5",
        max_tokens=8192,
        system=[
            {
                "type": "text",
                "text": _SYSTEM + "\n\n# Skill Reference\n\n" + _SKILL_CONTEXT,
                "cache_control": {"type": "ephemeral"},
            }
        ],
        messages=[{"role": "user", "content": user_prompt}],
    )
    return next(b.text for b in response.content if b.type == "text")


# ---------------------------------------------------------------------------
# Subagent 1 — Detect
# ---------------------------------------------------------------------------

def detect_agent(file_content: str, file_path: str = "unknown") -> dict:
    """
    Analyse a PySpark script and report detected PII/demographic columns.

    Returns a dict with keys:
        detected  : list[str]   – column names that match PII patterns
        clean     : list[str]   – remaining columns
        summary   : str         – human-readable explanation
    """
    prompt = f"""
File: {file_path}

```python
{file_content}
```

Task: identify every column name in this PySpark script that contains
personally-identifiable or demographic data (names, SSN, email, phone,
address, DOB, gender, race, account numbers, medical fields, IP address, etc.).

Return a JSON object with exactly these keys:
  "detected"  – list of PII column names found in the script
  "clean"     – list of non-PII column names found in the script
  "summary"   – one sentence describing what was found

Output ONLY the JSON object, no markdown.
"""
    import json
    raw = _call(prompt)
    # strip any accidental markdown fences
    raw = raw.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
    try:
        result = json.loads(raw)
    except json.JSONDecodeError:
        result = {"detected": [], "clean": [], "summary": raw}
    return result


# ---------------------------------------------------------------------------
# Subagent 2 — Encrypt
# ---------------------------------------------------------------------------

def encrypt_agent(
    file_content: str,
    file_path: str = "unknown",
    columns: list[str] | None = None,
    encrypt_all_detected: bool = False,
    mode: str = "hash",
) -> str:
    """
    Modify a PySpark script to encrypt/hash PII columns using DemographicsHandler.

    Returns the complete modified Python source as a string.
    """
    col_instruction = (
        f"Encrypt only these columns: {columns}"
        if columns
        else (
            "Use encrypt_all_detected=True to encrypt every auto-detected PII column."
            if encrypt_all_detected
            else "Encrypt all auto-detected PII columns."
        )
    )

    key_snippet = (
        'encryption_key=os.environ["DEMOGRAPHICS_ENCRYPTION_KEY"], mode="aes"'
        if mode == "aes"
        else 'mode="hash"'
    )

    aes_import = "import os\n" if mode == "aes" else ""

    prompt = f"""
File: {file_path}

```python
{file_content}
```

Task: add DemographicsHandler to encrypt PII columns.

Rules:
1. Add `{_HANDLER_IMPORT}` near the top imports.
2. {aes_import.strip() + " Add `import os`" if mode == "aes" else "No extra imports needed."}
3. After the DataFrame is created/loaded, add:

   handler = DemographicsHandler({key_snippet})
   df = handler.encrypt_columns(df, {
    f"columns={columns}" if columns else "encrypt_all_detected=True"
   })

4. {col_instruction}
5. Keep every other line exactly as-is.

Return the COMPLETE modified Python source code only.
"""
    return _call(prompt)


# ---------------------------------------------------------------------------
# Subagent 3 — Remove
# ---------------------------------------------------------------------------

def remove_agent(
    file_content: str,
    file_path: str = "unknown",
    columns: list[str] | None = None,
) -> str:
    """
    Modify a PySpark script to drop PII columns using DemographicsHandler.

    Returns the complete modified Python source as a string.
    """
    extra = f", extra_columns={columns}" if columns else ""

    prompt = f"""
File: {file_path}

```python
{file_content}
```

Task: add DemographicsHandler to remove/drop all PII demographic columns.

Rules:
1. Add `{_HANDLER_IMPORT}` near the top imports.
2. After the DataFrame is created/loaded, add:

   handler = DemographicsHandler()
   df = handler.remove_demographics(df{extra})

3. Keep every other line exactly as-is.

Return the COMPLETE modified Python source code only.
"""
    return _call(prompt)


# ---------------------------------------------------------------------------
# Subagent 4 — Redact
# ---------------------------------------------------------------------------

def redact_agent(
    file_content: str,
    file_path: str = "unknown",
    columns: list[str] | None = None,
    redact_value: str = "***REDACTED***",
) -> str:
    """
    Modify a PySpark script to redact PII column values using DemographicsHandler.

    Returns the complete modified Python source as a string.
    """
    col_arg = f", columns={columns}" if columns else ""
    val_arg = f', redact_value="{redact_value}"' if redact_value != "***REDACTED***" else ""

    prompt = f"""
File: {file_path}

```python
{file_content}
```

Task: add DemographicsHandler to replace PII column values with a redaction string.

Rules:
1. Add `{_HANDLER_IMPORT}` near the top imports.
2. After the DataFrame is created/loaded, add:

   handler = DemographicsHandler()
   df = handler.redact_demographics(df{col_arg}{val_arg})

3. Keep every other line exactly as-is.

Return the COMPLETE modified Python source code only.
"""
    return _call(prompt)
