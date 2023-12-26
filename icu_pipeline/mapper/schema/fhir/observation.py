from typing import Annotated

import pandas as pd
from pandera.typing import Series

from icu_pipeline.mapper.schema.fhir import (
    AbstractFHIRSinkSchema,
    Reference,
    Quantity,
    CodeableConcept,
)


class FHIRObservation(AbstractFHIRSinkSchema):
    _SINK_NAME = "observation"

    code: Series[CodeableConcept]  # type: ignore[type-var]
    subject: Series[Reference]  # type: ignore[type-var]
    effective_date_time: Series[Annotated[pd.DatetimeTZDtype, "ns", "utc"]]
    value_quantity: Series[Quantity]  # type: ignore[type-var]
