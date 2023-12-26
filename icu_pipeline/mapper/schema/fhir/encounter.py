from pandera.typing import Series

from icu_pipeline.mapper.schema.fhir import (
    AbstractFHIRSinkSchema,
    Reference,
    Period,
)


class FHIREncounter(AbstractFHIRSinkSchema):
    _SINK_NAME = "encounter"

    subject: Series[Reference]  # type: ignore[type-var]
    actual_period: Series[Period]  # type: ignore[type-var]
    care_team: Series[Reference]  # type: ignore[type-var]
