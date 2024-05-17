from abc import ABCMeta
from typing import Generator, Type, Any

import pandas as pd
from pandera.typing import DataFrame
from psycopg import sql


from icu_pipeline.mapper.source import AbstractDatabaseSourceMapper
from icu_pipeline.mapper.schema.fhir import (
    CodeableConcept,
    Coding,
    Reference,
    Quantity,
)
from icu_pipeline.mapper.schema.fhir.observation import FHIRObservation


class ObervationMapper(AbstractDatabaseSourceMapper[FHIRObservation]):
    def __init__(
        self,
        concept_id: str,
        concept_type: str,
        fhir_schema: Type[FHIRObservation],
        source_config,
        **kwargs: dict[str, Any],
    ) -> None:
        super().__init__(concept_id, concept_type, fhir_schema, source_config)

        self.SQL_QUERY = self.build_query(
            kwargs.get("schema"),
            kwargs.get("table"),
            kwargs.get("fields"),
            kwargs.get("constraints"),
        )

        self._unit = kwargs.get("unit", "undefined")
        self._source = kwargs.get("source")

    def _to_fihr(
        self, df: DataFrame
    ) -> Generator[DataFrame[FHIRObservation], None, None]:
        observation_df = pd.DataFrame()

        observation_df[FHIRObservation.subject] = df["patient_id"].map(
            lambda id: Reference(reference=str(id), type=f"{self._source}-patient")
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
