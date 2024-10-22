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
from icu_pipeline.unit.gender import Gender


class MimicObservationMapper(AbstractDatabaseSourceMapper[FHIRObservation]):
    """
    Mapper class that maps the MIMIC-IV data to the FHIR Observation schema.
    """

    def __init__(
        self,
        schema: str,
        table: str,
        constraints: dict[str, Any],
        unit: str,
        joins: dict[str, dict[str, str]] | None = None,
        **kwargs: dict[str, Any],
    ) -> None:
        super().__init__(fhir_schema=FHIRObservation, datasource=DataSource.MIMICIV, **kwargs)
        self._source = "mimiciv"
        self._unit = unit
        assert self._unit is not None, f"No Unit definition for MimicObservationMapper '{schema+'.'+table}' given."

        self._id_field = "subject_id"
        # Create and map fields to normalized names
        fields = kwargs.pop("fields", {})
        if "patient_id" not in fields:
            fields["patient_id"] = "subject_id"
        if "timestamp" not in fields:
            fields["timestamp"] = "charttime"
        if "value" not in fields:
            fields["value"] = "valuenum"
        self._query_args = {
            "schema": schema,
            "table": table,
            "constraints": constraints,
            "fields": fields,
            "joins": joins,
        }

        self._converter = self._convert_none
        if fields.get("value") == "gender":
            self._converter = self._convert_gender

    def _convert_none(self, value: str) -> str:
        return value

    def _convert_gender(self, gender: str) -> str:
        match gender:
            case "M":
                return str(Gender.MALE.value)
            case "F":
                return str(Gender.FEMALE.value)
            case _:
                return str(Gender.DIVERSE.value)

    def _to_fihr(self, df: DataFrame) -> DataFrame[FHIRObservation]:
        observation_df = pd.DataFrame()

        observation_df[FHIRObservation.subject] = df["patient_id"].map(
            lambda id: Reference(reference=str(id), type=f"{self._data_source}")
        )
        observation_df[FHIRObservation.effective_date_time] = pd.to_datetime(df["timestamp"], utc=True)
        observation_df[FHIRObservation.value_quantity] = df.apply(
            lambda _df: Quantity(
                value=float(self._converter(_df["value"])),
                unit=self._unit or self._unit,
            ),
            axis=1,
        )
        observation_df[FHIRObservation.code] = [
            CodeableConcept(coding=Coding(code=self._concept_id, system=self._concept_type))
        ] * len(df)

        return observation_df.pipe(DataFrame[FHIRObservation])
