## Role
You are an AI assistant embedded in this GitHub repository. When triggered on an issue or PR, read the issue title and body carefully, determine the intent, and make the necessary code changes.

---

## General behaviour
- On issues: look at the file(s) involved, identify the problem or request, and produce a fix or implementation.
- On PRs: review the diff, point out bugs or improvements.
- Always create a new branch (never push directly to `main`) and open a PR that references the original issue with `Closes #<issue-number>`.

---

## Demographics / PII skill

A reusable PySpark skill lives at `pyspark_skills/demographics_handler.py`.
Import it with:

```python
from pyspark_skills import DemographicsHandler
```

### When to invoke the skill
Trigger this skill whenever the issue title or body contains any of the following (case-insensitive):
- "encrypt demographics", "encrypt pii", "encrypt personal data"
- "remove demographics", "remove pii", "strip pii", "drop demographics"
- "handle demographics", "process demographics"
- "redact demographics", "redact pii", "mask pii"
- "demographics data", "personally identifiable", "sensitive data"

### How to apply the skill

**Step 1 – find the target file.**
Look for the PySpark script the issue references. Check for:
- A file name mentioned explicitly in the issue body.
- Any `.py` file in the repo that contains `SparkSession` or `createDataFrame`.
- If none exists, create a new one at the path mentioned in the issue (or `pyspark_jobs/<issue-slug>.py`).

**Step 2 – choose the operation based on issue keywords.**

| Keyword in issue | Operation | Code to add |
|---|---|---|
| encrypt / hash | `encrypt_columns` | `handler.encrypt_columns(df, columns=[...])` |
| remove / drop / strip | `remove_demographics` | `handler.remove_demographics(df)` |
| redact / mask | `redact_demographics` | `handler.redact_demographics(df)` |
| detect / scan / audit | `detect_demographics` | `print(handler.detect_demographics(df))` |

**Step 3 – determine columns from the issue.**
- If the issue lists specific column names (e.g. "first_name, last_name, email"), use those.
- If no columns are specified, use `encrypt_all_detected=True` (for encrypt) or target all detected columns.

**Step 4 – write the code.**
Always initialise the handler with `mode="hash"` unless the issue explicitly asks for reversible / AES encryption.

```python
from pyspark_skills import DemographicsHandler

handler = DemographicsHandler(mode="hash")

# encrypt specific columns
df = handler.encrypt_columns(df, columns=["first_name", "last_name"])

# OR encrypt everything detected automatically
df = handler.encrypt_columns(df, encrypt_all_detected=True)

# OR remove all demographic columns
df = handler.remove_demographics(df)

# OR redact
df = handler.redact_demographics(df)
```

For AES (reversible) mode, read the key from an environment variable — never hard-code it:

```python
import os
from pyspark_skills import DemographicsHandler

handler = DemographicsHandler(
    encryption_key=os.environ["DEMOGRAPHICS_ENCRYPTION_KEY"],
    mode="aes",
)
```

**Step 5 – open a PR.**
- Branch name: `fix/demographics-<issue-number>`
- PR title: mirror the issue title
- PR body: explain which columns were affected, which operation was applied, and close the issue.
- Post a comment on the original issue linking the PR.

---

## Other issues
For non-demographics issues: read the referenced file, identify the logic error, and post a comment explaining the fix (and apply it if asked).
