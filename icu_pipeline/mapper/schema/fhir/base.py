from typing import TypedDict, Annotated

import pandas as pd
from pydantic import PlainValidator

from icu_pipeline.mapper.schema import AbstractSinkSchema


class Reference(TypedDict):
    reference: str
    type: str


class Quantity(TypedDict):
    value: float
    unit: str


class Period(TypedDict):
    start: Annotated[pd.Timestamp, PlainValidator(lambda x: pd.Timestamp(x))]
    end: Annotated[pd.Timestamp, PlainValidator(lambda x: pd.Timestamp(x))]


class Coding(TypedDict):
    code: str
    system: str


class CodeableConcept(TypedDict):
    coding: Coding


class CodeableReference(TypedDict):
    concept: CodeableConcept


class AbstractFHIRSinkSchema(AbstractSinkSchema):
    pass
