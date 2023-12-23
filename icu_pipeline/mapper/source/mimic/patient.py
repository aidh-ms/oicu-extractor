from abc import ABCMeta

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


class AbstractPatientMapper(
    AbstractDatabaseSourceMapper[FHIRObservation, AbstractOHDSISinkSchema],
    metaclass=ABCMeta,
):
    SQL_QUERY = "SELECT * FROM mimiciv_hosp.patients;"
    SQL_PARAMS = {}
    VALUE_FIELD: str
    UNIT: str = ""

    def _to_fihr(self, df: DataFrame) -> DataFrame[FHIRObservation]:
        observation_df = pd.DataFrame()

        observation_df[FHIRObservation.subject] = df["subject_id"].map(
            lambda id: Reference(reference=str(id), type="Patient")
        )
        observation_df[FHIRObservation.effective_date_time] = pd.to_datetime(
            df["anchor_year"], format="%Y", utc=True  # TODO: Use first encouter date
        )
        observation_df[FHIRObservation.value_quantity] = df.apply(
            lambda _df: Quantity(value=float(_df[self.VALUE_FIELD]), unit=self.UNIT),
            axis=1,
        )
        observation_df[FHIRObservation.code] = [
            CodeableConcept(coding=Coding(code=self._snomed_id, system="snomed"))
        ] * len(df)

        return observation_df.pipe(DataFrame[FHIRObservation])

    def _to_ohdsi(self, df: DataFrame) -> DataFrame[AbstractOHDSISinkSchema]:
        raise NotImplementedError


class AgeMapper(AbstractPatientMapper):
    VALUE_FIELD = "anchor_age"
    UNIT = "years"


class GenderMapper(AbstractPatientMapper):
    VALUE_FIELD = "gender"
