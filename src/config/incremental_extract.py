"""
Incremental extract configuration (fetch-stage) for database resources.

v1 supports ``kind: jdbc_companion_cdc`` only. See AGENTS.md for conventions.
"""

from enum import StrEnum
from typing import List, Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class IncrementalExtractKind(StrEnum):
    """Discriminant for incremental extract strategies (extend for non-JDBC sources)."""

    JDBC_COMPANION_CDC = "jdbc_companion_cdc"


class IncrementalWatermarkCursorStrategy(StrEnum):
    """How the lower bound for the companion watermark is obtained for the next run."""

    DESTINATION_COLUMN = "destination_column"
    TABLE_METADATA = "table_metadata"


class IncrementalWatermarkCursorConfig(BaseModel):
    """
    Read a bound from the written Delta table to compare against the companion watermark
    (``watermark.column``) on the next run: ``companion.<column> > <bound>``.
    """

    strategy: IncrementalWatermarkCursorStrategy = Field(
        description=(
            "destination_column: use MAX(reference_column) from the destination Delta table. "
            "table_metadata: reserved; not implemented in v1."
        ),
    )
    reference_column: str = Field(
        description=(
            "Column name in the **written** table (``SchemaField.name`` after projection) whose "
            "MAX value seeds the JDBC bound for the companion watermark."
        ),
    )
    reference_format: Literal["yyyymmdd", "none"] = Field(
        default="none",
        description=(
            "When ``yyyymmdd``, MAX values are parsed as UTC calendar dates for tolerance math. "
            "Required when ``tolerance_calendar_days`` is greater than zero."
        ),
    )
    tolerance_calendar_days: int = Field(
        default=0,
        ge=0,
        description=(
            "Subtract this many calendar days from MAX(reference_column) after parsing when "
            "``reference_format`` is ``yyyymmdd``. Must be zero when ``reference_format`` is ``none``."
        ),
    )

    @field_validator("reference_column", mode="before")
    @classmethod
    def strip_reference_column(cls, v: object) -> str:
        s = str(v).strip() if v is not None else ""
        if not s:
            raise ValueError(
                "incremental_extract.watermark.cursor.reference_column must be non-empty"
            )
        return s

    @model_validator(mode="after")
    def reject_unimplemented_and_tolerance_rules(self) -> "IncrementalWatermarkCursorConfig":
        if self.strategy == IncrementalWatermarkCursorStrategy.TABLE_METADATA:
            raise ValueError(
                "incremental_extract watermark.cursor.strategy 'table_metadata' is not implemented; "
                "use 'destination_column'"
            )
        if self.tolerance_calendar_days > 0 and self.reference_format != "yyyymmdd":
            raise ValueError(
                "incremental_extract.watermark.cursor.tolerance_calendar_days > 0 requires "
                "watermark.cursor.reference_format 'yyyymmdd' so the bound can be shifted safely."
            )
        return self


class IncrementalWatermarkConfig(BaseModel):
    """Companion-side watermark used in JDBC predicates."""

    column: str = Field(description="Watermark column on the companion (CDC) table.")
    ordering: Literal["lexical", "numeric", "timestamp"] = Field(
        default="lexical",
        description="How values are compared in SQL (lexical for SAP REQTSN-style strings).",
    )
    cursor: IncrementalWatermarkCursorConfig

    @field_validator("column", mode="before")
    @classmethod
    def strip_column(cls, v: object) -> str:
        s = str(v).strip()
        if not s:
            raise ValueError("incremental_extract.watermark.column must be non-empty")
        return s


class IncrementalCorrelationConfig(BaseModel):
    """How the main table row relates to the companion CDC row."""

    companion_metadata_columns: Optional[List[str]] = Field(
        default=None,
        description=(
            "Companion-only columns (not on the main table), e.g. DATAPAKID, RECORD, REQTSN. "
            "Used for validation: they must not appear in ``fields`` except when the same "
            "physical name is intentionally projected under a different field name."
        ),
    )
    join_columns: Optional[List[str]] = Field(
        default=None,
        description="Explicit equi-join keys shared by main and companion; omit to infer at runtime.",
    )
    join_predicate: Optional[str] = Field(
        default=None,
        description="SQL boolean using aliases ``m`` (main) and ``c`` (companion). Mutually exclusive with join_columns.",
    )

    @model_validator(mode="after")
    def join_exclusive(self) -> "IncrementalCorrelationConfig":
        if self.join_predicate and self.join_columns:
            raise ValueError(
                "incremental_extract.correlation: use either join_columns or join_predicate, not both"
            )
        if self.join_columns is not None and len(self.join_columns) == 0:
            raise ValueError(
                "incremental_extract.correlation.join_columns must be non-empty when set"
            )
        return self


class IncrementalCompanionConfig(BaseModel):
    """CDC / changelog table paired with the main extract table."""

    schema: Optional[str] = Field(
        default=None,
        description="Companion schema; defaults to the resource database_schema when omitted.",
    )
    table: str = Field(description="Companion table name (same rules as database_table).")

    @field_validator("schema", "table", mode="before")
    @classmethod
    def strip_identifiers(cls, v: object) -> Optional[str]:
        if v is None:
            return None
        s = str(v).strip()
        if s == "":
            return None
        return s

    @model_validator(mode="after")
    def require_table(self) -> "IncrementalCompanionConfig":
        if not self.table or not str(self.table).strip():
            raise ValueError("incremental_extract.companion.table is required")
        return self


class IncrementalExtractConfig(BaseModel):
    """Fetch-stage incremental configuration for database resources."""

    kind: IncrementalExtractKind = Field(
        default=IncrementalExtractKind.JDBC_COMPANION_CDC,
        description="jdbc_companion_cdc: bound reads using a companion CDC table and watermark column.",
    )
    companion: IncrementalCompanionConfig
    watermark: IncrementalWatermarkConfig
    correlation: IncrementalCorrelationConfig = Field(default_factory=IncrementalCorrelationConfig)

    @model_validator(mode="after")
    def kind_supported(self) -> "IncrementalExtractConfig":
        if self.kind != IncrementalExtractKind.JDBC_COMPANION_CDC:
            raise ValueError(
                f"incremental_extract.kind {self.kind.value!r} is not supported in this version"
            )
        return self
