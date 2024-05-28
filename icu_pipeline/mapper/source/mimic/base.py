from typing import Any

from icu_pipeline.mapper.source.base import ObservationMapper


class MimicObservationMapper(ObservationMapper):
    """
    Mapper class that maps the MIMIC-IV data to the FHIR Observation schema.
    """

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
