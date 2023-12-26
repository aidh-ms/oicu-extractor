from typing import Annotated

import pandas as pd
from pandera.typing import Series

from icu_pipeline.mapper.schema.fhir import (
    AbstractFHIRSinkSchema,
    Reference,
    Quantity,
    CodeableReference,
    Period,
)


class FHIRDeviceUsage(AbstractFHIRSinkSchema):
    _SINK_NAME = "deviceusage"

    patient: Series[Reference]  # type: ignore[type-var]
    timing_date_time: Series[Annotated[pd.DatetimeTZDtype, "ns", "utc"]]
    device: Series[CodeableReference]  # type: ignore[type-var]
