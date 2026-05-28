# PySpark Skills — Demographics Handler

## Skill: `demographics_handler`

### Purpose
Detect, encrypt, remove, or redact personally identifiable information (PII) and demographic columns in a PySpark DataFrame.

---

## Trigger Keywords
This skill activates when any of the following phrases appear in a GitHub issue title or body (case-insensitive):

| Phrase | Operation triggered |
|---|---|
| encrypt demographics / encrypt pii | Encrypt demographic columns |
| remove demographics / remove pii / drop pii / strip pii | Drop demographic columns |
| redact demographics / redact pii / mask pii | Replace values with redaction string |
| detect demographics / scan pii / audit pii | Report detected columns only |
| handle demographics / process demographics | Detect + encrypt by default |
| demographics data / personally identifiable / sensitive data | Detect + encrypt by default |

---

## Capabilities

### 1. Detect
Scan a DataFrame and identify which columns contain demographic or PII data based on column name patterns.

**What it detects:**

| Category | Example column names |
|---|---|
| Name | first_name, last_name, full_name, middle_name |
| Identity | ssn, social_security, tax_id, passport, drivers_license |
| Contact | email, phone, mobile, cell_phone, fax |
| Address | address, street, city, state, zip, postal_code, county |
| Date of birth | dob, date_of_birth, birth_date, birth_year, age |
| Demographics | gender, sex, race, ethnicity, religion, marital_status |
| Financial | account_number, credit_card, bank_account, routing_number |
| Medical | diagnosis, condition, medication, icd_code, mrn, patient_id |
| Device / Network | ip_address, mac_address |

**Output:** Two lists — `detected` (PII columns) and `clean` (safe columns).

---

### 2. Encrypt
Replace column values with an encrypted or hashed representation.

**Modes:**

| Mode | Algorithm | Reversible | Spark version |
|---|---|---|---|
| `hash` | SHA-256 | No (one-way) | All versions |
| `aes` | AES-GCM | Yes (needs key) | 3.3+ |

**Column selection:**
- Explicit list of column names from the issue.
- If no columns specified → encrypt all detected demographic columns automatically.

**Key handling (AES mode only):**
- Key must be 16, 24, or 32 characters.
- Always read from an environment variable — never hard-coded.

---

### 3. Remove
Drop all detected demographic columns from the DataFrame entirely.

- Accepts an additional explicit list of columns to drop alongside auto-detected ones.
- Safe — only drops columns that actually exist in the DataFrame.

---

### 4. Redact
Replace demographic column values with a static placeholder string (default: `***REDACTED***`).

- Useful for audit trails where the column must remain present but values must be hidden.
- Replacement string is configurable.

---

## Decision Logic

```
Issue received
    │
    ├── keyword: encrypt / hash        →  encrypt_columns()
    ├── keyword: remove / drop / strip →  remove_demographics()
    ├── keyword: redact / mask         →  redact_demographics()
    ├── keyword: detect / scan / audit →  detect_demographics()  (no file change)
    └── keyword: handle / process      →  detect + encrypt_columns()
```

If specific column names are mentioned in the issue → use those columns.
If no columns mentioned → apply the operation to all auto-detected columns.

---

## Inputs

| Input | Source | Required |
|---|---|---|
| Target PySpark file | Named in issue body, or auto-discovered (any `.py` with `SparkSession`) | Yes |
| Operation | Derived from issue keywords | Yes |
| Column list | Parsed from issue body | No — falls back to auto-detect |
| Encryption key (AES) | Environment variable `DEMOGRAPHICS_ENCRYPTION_KEY` | Only for AES mode |

---

## Outputs

| Output | Description |
|---|---|
| Modified `.py` file | Target file updated with `DemographicsHandler` applied |
| New branch | `fix/demographics-<issue-number>` |
| Pull Request | References and closes the original issue |
| Issue comment | Posted on the original issue with a link to the PR |

---

## Constraints

- Never push directly to `main`.
- Never hard-code encryption keys.
- Default to `mode="hash"` unless the issue explicitly requests reversible / AES encryption.
- Only modify columns that exist in the DataFrame — skip missing ones silently.
- The skill lives at `pyspark_skills/demographics_handler.py` and is imported as `from pyspark_skills import DemographicsHandler`.
