from typing import Any

import pandas as pd
from pandera.typing import DataFrame

from icu_pipeline.source import DataSource
from icu_pipeline.source.database import AbstractDatabaseSourceMapper
from icu_pipeline.schema.fhir import (
    CodeableConcept,
    Coding,
    Reference,
    Quantity,
)
from icu_pipeline.schema.fhir.observation import FHIRObservation
from icu_pipeline.source.utils import offset_to_timestamp, to_timestamp


class EICUObservationMapper(AbstractDatabaseSourceMapper[FHIRObservation]):
    """
    Mapper class that maps the EICU data to the FHIR Observation schema.
    """

    def __init__(
        self,
        schema: str,
        table: str,
        unit: str,
        constraints: dict[str, Any] | None = None,
        **kwargs: dict[str, Any],
    ) -> None:
        super().__init__(
            fhir_schema=FHIRObservation, datasource=DataSource.EICU, **kwargs
        )
        self._source = "eicu"
        self._unit = unit
        assert (
            self._unit is not None
        ), f"No Unit definition for EICUObservationMapper '{schema+'.'+table}' given."
        if constraints is None:
            constraints = {}

        self._id_field = "subject_id"
        # Create and map fields to normalized names
        fields = kwargs.pop("fields", {})
        assert (
            fields.get("value") is not None
        ), f"No constraints for EICUObservationMapper '{schema+'.'+table}' given."
        constraints[fields.get("value")] = "not null"

        if "patient_id" not in fields:
            fields["patient_id"] = "patienthealthsystemstayid"
        if "time" not in fields:
            fields["time"] = "hospitaladmittime24"
        if "year" not in fields:
            fields["year"] = "hospitaldischargeyear"
        if "time" not in fields:
            fields["time"] = "hospitaladmittime24"
        if "offset" not in fields:
            fields["offset"] = "observationoffset"

        self._query_args = {
            "schema": schema,
            "table": table,
            "constraints": constraints,
            "fields": fields,
            "joins": {
                "eicu_crd.patient": {
                    "eicu_crd.vitalperiodic.patientunitstayid": "eicu_crd.patient.patientunitstayid"
                }
            },
        }

    def _to_fihr(self, df: DataFrame) -> DataFrame[FHIRObservation]:
        observation_df = pd.DataFrame()

        observation_df[FHIRObservation.subject] = df["patient_id"].map(
            lambda id: Reference(reference=str(id), type=f"{self._data_source}")
        )
        observation_df[FHIRObservation.effective_date_time] = df.apply(
            lambda _df: offset_to_timestamp(
                to_timestamp(_df["time"], _df["year"]),
                _df["offset"],
            ),
            axis=1,
        )
        observation_df[FHIRObservation.value_quantity] = df.apply(
            lambda _df: Quantity(
                value=float(_df["value"]), unit=self._unit or self._unit
            ),
            axis=1,
        )
        observation_df[FHIRObservation.code] = [
            CodeableConcept(
                coding=Coding(code=self._concept_id, system=self._concept_type)
            )
        ] * len(df)

        return observation_df.pipe(DataFrame[FHIRObservation])
