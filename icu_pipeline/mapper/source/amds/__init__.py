from abc import ABCMeta
from venv import logger

import pandas as pd
from pandera.typing import DataFrame

from icu_pipeline.mapper.source import AbstractDatabaseSourceMapper
from icu_pipeline.mapper.schema.ohdsi import AbstractOHDSISinkSchema
from icu_pipeline.mapper.schema.fhir import (
    CodeableConcept,
    Coding,
    Reference,
    Quantity,
)
from icu_pipeline.mapper.schema.fhir.observation import FHIRObservation


class AbstractAmdsEventsMapper(
    AbstractDatabaseSourceMapper[FHIRObservation, AbstractOHDSISinkSchema],
    metaclass=ABCMeta,
):
    UNIT = ""

    def _to_fihr(self, df: DataFrame) -> DataFrame[FHIRObservation]:
        observation_df = pd.DataFrame()

        observation_df[FHIRObservation.subject] = df["admissionid"].map(
            lambda id: Reference(reference=str(id), type="AMDS-Patient")
        )
        observation_df[FHIRObservation.effective_date_time] = pd.to_datetime(
            df["measuredat"], utc=True
        )
        observation_df[FHIRObservation.value_quantity] = df.apply(
            lambda _df: Quantity(
                value=float(_df["value"]), unit=_df["unit"] or self.UNIT
            ),
            axis=1,
        )
        observation_df[FHIRObservation.code] = [
            CodeableConcept(
                coding=Coding(code=self._concept_id, system=self._concept_type)
            )
        ] * len(df)

        return observation_df.pipe(DataFrame[FHIRObservation])

    def _to_ohdsi(self, df: DataFrame) -> DataFrame[AbstractOHDSISinkSchema]:
        raise NotImplementedError
