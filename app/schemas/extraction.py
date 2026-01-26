# app/schemas/extraction.py
from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional

# Compat Pydantic v1/v2 sin refactor grande
try:
    # Pydantic v2
    from pydantic import BaseModel, Field, field_validator, model_validator
    PYDANTIC_V2 = True
except Exception:  # pragma: no cover
    # Pydantic v1
    from pydantic import BaseModel, Field, validator as field_validator, root_validator as model_validator
    PYDANTIC_V2 = False


class Uom(str, Enum):
    UND = "UND"
    M = "M"
    KG = "KG"
    ROL = "ROL"
    EA = "EA"
    BOX = "BOX"
    SET = "SET"
    L = "L"
    GAL = "GAL"
    PACK = "PACK"


class ExtractedItem(BaseModel):
    line_index: int = Field(..., ge=0)
    raw_text: str = Field(..., min_length=1)
    description: str = Field(..., min_length=1)
    quantity: float = Field(..., gt=0)
    uom: Uom
    uom_raw: Optional[str] = None
    confidence: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    warnings: Optional[List[str]] = None

    # --- Validators ---
    if PYDANTIC_V2:
        @field_validator("raw_text", "description")
        @classmethod
        def _strip_non_empty(cls, v: str) -> str:
            v2 = (v or "").strip()
            if not v2:
                raise ValueError("must not be empty")
            return v2

        @field_validator("warnings")
        @classmethod
        def _warnings_default(cls, v: Optional[List[str]]) -> Optional[List[str]]:
            if v is None:
                return None
            return [str(x).strip() for x in v if str(x).strip()]
    else:
        @field_validator("raw_text")
        def _raw_text_non_empty(cls, v: str) -> str:  # type: ignore
            v2 = (v or "").strip()
            if not v2:
                raise ValueError("raw_text must not be empty")
            return v2

        @field_validator("description")
        def _description_non_empty(cls, v: str) -> str:  # type: ignore
            v2 = (v or "").strip()
            if not v2:
                raise ValueError("description must not be empty")
            return v2

        @field_validator("warnings")
        def _warnings_clean(cls, v: Optional[List[str]]):  # type: ignore
            if v is None:
                return None
            return [str(x).strip() for x in v if str(x).strip()]


class ExtractionResult(BaseModel):
    items: List[ExtractedItem] = Field(default_factory=list)
    global_warnings: Optional[List[str]] = None
    meta: Optional[Dict[str, Any]] = None

    if PYDANTIC_V2:
        @field_validator("global_warnings")
        @classmethod
        def _global_warnings_clean(cls, v: Optional[List[str]]) -> Optional[List[str]]:
            if v is None:
                return None
            return [str(x).strip() for x in v if str(x).strip()]

        @model_validator(mode="after")
        def _validate_items_non_empty_desc(self):
            # Ya se valida por item; aquí solo dejamos hook futuro
            return self
    else:
        @field_validator("global_warnings")
        def _global_warnings_clean(cls, v: Optional[List[str]]):  # type: ignore
            if v is None:
                return None
            return [str(x).strip() for x in v if str(x).strip()]
