from typing import Any

import pandas as pd
from pandera.typing import DataFrame

from icu_pipeline.source import SourceConfig
from icu_pipeline.source.database import AbstractDatabaseSourceSampler, AbstractDatabaseSourceMapper
from icu_pipeline.schema.fhir import (
    CodeableConcept,
    Coding,
    Reference,
    Quantity
)
from icu_pipeline.schema.fhir.observation import FHIRObservation


class MimicSampler(AbstractDatabaseSourceSampler):
    # TODO - subject IDs have an arbitrary amount of admissions..
    #   Use both, subject_id + admission_id?
    IDENTIFIER = ["subject_id"] # subject_id, stay_id

    def __init__(self, source_config: SourceConfig) -> None:
        super().__init__(source_config)

        self.SQL_QUERY = self.build_query(
            schema="mimiciv_icu",
            table="icustays",
        )


class MimicObservationMapper(AbstractDatabaseSourceMapper[FHIRObservation]):
    """
    Mapper class that maps the MIMIC-IV data to the FHIR Observation schema.
    """
    IDENTIFIER = MimicSampler.IDENTIFIER

    def __init__(
        self,
        schema: str,
        table: str,
        constraints: dict[str, Any],
        **kwargs: dict[str, Any],
    ) -> None:
        super().__init__(**kwargs)
        self._source = kwargs.get("source")
        self._unit = kwargs.get("unit", "undefined")
        
        self._id_field = "subject_id"
        self._query_args = {
            "schema": schema,
            "table": table,
            "constraints": constraints,
            "fields": {
                "patient_id": "subject_id",
                "timestamp": "charttime",
                "value": "valuenum",
                "unit": "valueuom",
            }
        }

    def _to_fihr(
        self, df: DataFrame
    ) -> DataFrame[FHIRObservation]:
        observation_df = pd.DataFrame()

        observation_df[FHIRObservation.subject] = df["patient_id"].map(
            lambda id: Reference(reference=str(id), type=f"{self._data_source}")
        )
        observation_df[FHIRObservation.effective_date_time] = pd.to_datetime(
            df["timestamp"], utc=True
        )
        observation_df[FHIRObservation.value_quantity] = df.apply(
            lambda _df: Quantity(
                value=float(_df["value"]), unit=_df["unit"] or self._unit
            ),
            axis=1,
        )
        observation_df[FHIRObservation.code] = [
            CodeableConcept(
                coding=Coding(code=self._concept_id, system=self._concept_type)
            )
        ] * len(df)

        return observation_df.pipe(DataFrame[FHIRObservation])
