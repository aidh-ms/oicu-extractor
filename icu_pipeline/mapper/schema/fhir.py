from typing import TypedDict, Annotated

import pandas as pd
from pandera.typing import Series

from icu_pipeline.mapper.schema import AbstractSinkSchema


class Identifier(TypedDict):
    value: str


class Subject(TypedDict):
    reference: str


class ValueQuantity(TypedDict):
    value: float
    unit: str


class AbstractFHIRSinkSchema(AbstractSinkSchema):
    identifier: Series[Identifier]


class FHIRObservation(AbstractFHIRSinkSchema):
    _SINK_NAME = "observation"

    subject: Series[Subject]
    effective_date_time: Series[Annotated[pd.DatetimeTZDtype, "ns", "utc"]]
    value_quantity: Series[ValueQuantity]
