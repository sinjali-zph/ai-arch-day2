"""
PySpark Demographics Handler
Detects, removes, and encrypts PII/demographic columns in a DataFrame.

Encryption modes:
  - "aes"  : AES-GCM (reversible), requires Spark 3.3+
  - "hash" : SHA-256 (one-way pseudonymisation), works on all Spark versions

Usage:
    from pyspark_skills.demographics_handler import DemographicsHandler

    handler = DemographicsHandler(encryption_key="my-secret-32-char-key!!!!!!!!!!!")
    clean_df = handler.remove_demographics(df)
    encrypted_df = handler.encrypt_columns(df, columns=["first_name", "last_name"])
    report = handler.detect_demographics(df)
"""

import re
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


# ---------------------------------------------------------------------------
# Column-name patterns considered demographic / PII
# ---------------------------------------------------------------------------
_DEMOGRAPHIC_PATTERNS: list[str] = [
    # Name
    r"first[\W_]?name", r"last[\W_]?name", r"middle[\W_]?name",
    r"full[\W_]?name", r"^name$", r"maiden[\W_]?name", r"suffix", r"prefix",
    # Identity
    r"ssn", r"social[\W_]?security", r"tax[\W_]?id", r"itin", r"passport",
    r"drivers?[\W_]?licen[sc]e", r"national[\W_]?id",
    # Contact
    r"email", r"e[\W_]?mail", r"phone", r"mobile", r"cell[\W_]?phone",
    r"fax", r"pager",
    # Address / Location
    r"address", r"addr", r"street", r"city", r"state", r"zip", r"postal",
    r"county", r"province", r"country",
    # Date of birth / age
    r"dob", r"date[\W_]?of[\W_]?birth", r"birth[\W_]?date", r"birth[\W_]?year",
    r"^age$",
    # Demographics
    r"gender", r"sex", r"race", r"ethnicity", r"religion", r"nationality",
    r"marital[\W_]?status",
    # Financial PII
    r"account[\W_]?number", r"acct[\W_]?no", r"credit[\W_]?card",
    r"bank[\W_]?account", r"routing[\W_]?number",
    # Medical
    r"diagnosis", r"condition", r"medication", r"icd[\W_]?code",
    r"mrn", r"patient[\W_]?id",
    # IP / device
    r"ip[\W_]?address", r"mac[\W_]?address",
]

_COMPILED = [re.compile(p, re.IGNORECASE) for p in _DEMOGRAPHIC_PATTERNS]


def _is_demographic_column(col_name: str) -> bool:
    return any(rx.search(col_name) for rx in _COMPILED)


# ---------------------------------------------------------------------------
# Main handler class
# ---------------------------------------------------------------------------

class DemographicsHandler:
    """
    Detects, removes, and encrypts demographic / PII columns in a PySpark DataFrame.

    Args:
        encryption_key: 16, 24, or 32-character key used for AES encryption.
                        Ignored when mode="hash".
        mode:           "aes" (reversible, Spark 3.3+) or "hash" (SHA-256, one-way).
        extra_patterns: Additional regex patterns to flag as demographic columns.
    """

    def __init__(
        self,
        encryption_key: str = "",
        mode: str = "hash",
        extra_patterns: Optional[list[str]] = None,
    ) -> None:
        if mode not in ("aes", "hash"):
            raise ValueError("mode must be 'aes' or 'hash'")
        if mode == "aes" and not encryption_key:
            raise ValueError("encryption_key is required when mode='aes'")
        if mode == "aes" and len(encryption_key) not in (16, 24, 32):
            raise ValueError("AES key must be 16, 24, or 32 characters")

        self.encryption_key = encryption_key
        self.mode = mode

        self._patterns = list(_COMPILED)
        if extra_patterns:
            self._patterns += [re.compile(p, re.IGNORECASE) for p in extra_patterns]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def detect_demographics(self, df: DataFrame) -> dict[str, list[str]]:
        """
        Scan a DataFrame and return a report of detected demographic columns.

        Returns:
            {
              "detected": ["col1", "col2", ...],
              "clean":    ["col3", "col4", ...]
            }
        """
        detected, clean = [], []
        for col in df.columns:
            (detected if self._is_demo(col) else clean).append(col)
        return {"detected": detected, "clean": clean}

    def remove_demographics(
        self,
        df: DataFrame,
        extra_columns: Optional[list[str]] = None,
    ) -> DataFrame:
        """
        Drop all detected demographic columns (plus any explicitly listed ones).

        Args:
            df:            Input DataFrame.
            extra_columns: Additional column names to always drop regardless of pattern.

        Returns:
            DataFrame with demographic columns removed.
        """
        to_drop = self.detect_demographics(df)["detected"]
        if extra_columns:
            to_drop = list(set(to_drop) | set(extra_columns))
        to_drop = [c for c in to_drop if c in df.columns]
        return df.drop(*to_drop) if to_drop else df

    def encrypt_columns(
        self,
        df: DataFrame,
        columns: Optional[list[str]] = None,
        encrypt_all_detected: bool = False,
    ) -> DataFrame:
        """
        Encrypt (or hash) the specified columns in-place.

        Args:
            df:                   Input DataFrame.
            columns:              Explicit list of column names to encrypt.
            encrypt_all_detected: When True, encrypt every detected demographic column
                                  instead of (or in addition to) the explicit list.

        Returns:
            DataFrame with the chosen columns replaced by their encrypted values.
        """
        targets: set[str] = set(columns or [])
        if encrypt_all_detected:
            targets |= set(self.detect_demographics(df)["detected"])

        # Only operate on columns that actually exist
        targets = {c for c in targets if c in df.columns}

        for col_name in targets:
            df = df.withColumn(col_name, self._encrypt_expr(col_name))
        return df

    def redact_demographics(
        self,
        df: DataFrame,
        redact_value: str = "***REDACTED***",
        columns: Optional[list[str]] = None,
    ) -> DataFrame:
        """
        Replace demographic column values with a static redaction string.
        Useful when neither removal nor encryption is appropriate.

        Args:
            df:           Input DataFrame.
            redact_value: The string to substitute.
            columns:      Explicit list; defaults to all detected demographics.

        Returns:
            DataFrame with demographic values replaced.
        """
        targets = columns if columns else self.detect_demographics(df)["detected"]
        targets = [c for c in targets if c in df.columns]
        for col_name in targets:
            df = df.withColumn(col_name, F.lit(redact_value).cast(StringType()))
        return df

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _is_demo(self, col_name: str) -> bool:
        return any(rx.search(col_name) for rx in self._patterns)

    def _encrypt_expr(self, col_name: str):
        """Return a Column expression that encrypts/hashes the given column."""
        if self.mode == "hash":
            # SHA-256: deterministic, one-way, no key required
            return F.sha2(F.col(col_name).cast(StringType()), 256)

        # AES-GCM (Spark 3.3+): reversible encryption
        # aes_encrypt(value, key, mode, padding)
        return F.aes_encrypt(
            F.col(col_name).cast(StringType()),
            F.lit(self.encryption_key),
            F.lit("GCM"),
        )
