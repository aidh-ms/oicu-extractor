from typing import TypedDict, Annotated

import pandas as pd
from pandera.typing import Series

from icu_pipeline.mapper.schema import AbstractSinkSchema


class Identifier(TypedDict):
    value: str
    system: str


class Reference(TypedDict):
    reference: str
    type: str


class Quantity(TypedDict):
    value: float
    unit: str


class AbstractFHIRSinkSchema(AbstractSinkSchema):
    identifier: Series[Identifier]  # type: ignore[type-var]
