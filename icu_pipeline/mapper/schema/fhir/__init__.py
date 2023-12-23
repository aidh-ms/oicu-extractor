from typing import TypedDict, Type

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


class CodeableConcept(TypedDict):
    coding: Coding


class AbstractFHIRSinkSchema(AbstractSinkSchema):
    pass
