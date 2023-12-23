from typing import TypedDict

from pandera.typing import Series

from icu_pipeline.mapper.schema.fhir import (
    AbstractFHIRSinkSchema,
    Reference,
    Quantity,
    CodeableConcept,
    Period,
)


class Dosage(TypedDict):
    dose_quantity: Quantity
    rate_quantity: Quantity


class FHIRMedicationStatement(AbstractFHIRSinkSchema):
    _SINK_NAME = "medicationstatement"

    medication: Series[CodeableConcept]  # type: ignore[type-var]
    subject: Series[Reference]  # type: ignore[type-var]
    effective_period: Series[Period]
    dosage: Series[Dosage]  # type: ignore[type-var]
