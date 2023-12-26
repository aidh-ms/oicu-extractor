from typing import TypedDict

from pandera.typing import Series

from icu_pipeline.mapper.schema.fhir import (
    AbstractFHIRSinkSchema,
    Reference,
    Quantity,
    CodeableReference,
    Period,
)


class Dosage(TypedDict):
    dose_quantity: Quantity
    rate_quantity: Quantity


class FHIRMedicationStatement(AbstractFHIRSinkSchema):
    _SINK_NAME = "medicationstatement"

    subject: Series[Reference]  # type: ignore[type-var]
    effective_period: Series[Period]  # type: ignore[type-var]
    medication: Series[CodeableReference]  # type: ignore[type-var]
    dosage: Series[Dosage]  # type: ignore[type-var]
