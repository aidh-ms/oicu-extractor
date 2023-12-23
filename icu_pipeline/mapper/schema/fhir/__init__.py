from typing import TypedDict, Annotated

import pandas as pd

from icu_pipeline.mapper.schema import AbstractSinkSchema


class Reference(TypedDict):
    reference: str
    type: str


class Quantity(TypedDict):
    value: float
    unit: str


class Coding(TypedDict):
    code: str
    system: str


class Period(TypedDict):
    start: Annotated[pd.DatetimeTZDtype, "ns", "utc"]
    end: Annotated[pd.DatetimeTZDtype, "ns", "utc"]


class CodeableConcept(TypedDict):
    coding: Coding


class CodeableConcept(TypedDict):
    concept: CodeableConcept


class AbstractFHIRSinkSchema(AbstractSinkSchema):
    pass
