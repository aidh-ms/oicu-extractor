from typing import Type, Any

from icu_pipeline.mapper.schema.fhir.observation import FHIRObservation
from icu_pipeline.mapper.source.base import ObervationMapper


class MimicObervationMapper(ObervationMapper):
    def __init__(
        self,
        **kwargs: dict[str, Any],
    ) -> None:
        super().__init__(
            fields={
                "patient_id": "subject_id",
                "timestamp": "charttime",
                "value": "valuenum",
                "unit": "valueuom",
            },
            **kwargs,
        )
