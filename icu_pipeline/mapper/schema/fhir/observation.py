from typing import TypedDict, Annotated

import pandas as pd
from pandera.typing import Series

from icu_pipeline.mapper.schema.fhir import AbstractFHIRSinkSchema, Reference, Quantity


class FHIRObservation(AbstractFHIRSinkSchema):
    _SINK_NAME = "observation"

    subject: Series[Reference]  # type: ignore[type-var]
    effective_date_time: Series[Annotated[pd.DatetimeTZDtype, "ns", "utc"]]
    value_quantity: Series[Quantity]  # type: ignore[type-var]
