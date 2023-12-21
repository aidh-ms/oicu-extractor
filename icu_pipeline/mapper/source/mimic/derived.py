from abc import ABCMeta

import pandas as pd
from pandera.typing import DataFrame

from icu_pipeline.mapper.source import AbstractDatabaseSourceMapper
from icu_pipeline.mapper.schema.ohdsi import AbstractOHDSISinkSchema
from icu_pipeline.mapper.schema.fhir import (
    Identifier,
    Reference,
    Quantity,
)
from icu_pipeline.mapper.schema.fhir.observation import FHIRObservation


class UrineOutputMapper(
    AbstractDatabaseSourceMapper[FHIRObservation, AbstractOHDSISinkSchema],
):
    SQL_QUERY = """
    SELECT 
        subject_id, charttime, SUM(CASE
            WHEN oe.itemid = 227488 AND oe.value > 0 THEN -1 * oe.value
            ELSE oe.value
        END) AS urineoutput
    FROM mimiciv_icu.outputevents oe
    WHERE itemid = any(%(values)s)
    GROUP BY subject_id, charttime;"""
    SQL_PARAMS = {"values": [226559, 226560, 226561, 226584, 226563, 226564, 226565, 226567, 226557, 226558, 227488, 227489]}  # fmt: skip

    def _to_fihr(self, df: DataFrame) -> DataFrame[FHIRObservation]:
        observation_df = pd.DataFrame()

        observation_df[FHIRObservation.subject] = df["subject_id"].map(
            lambda id: Reference(reference=str(id), type="Patient")
        )
        observation_df[FHIRObservation.effective_date_time] = pd.to_datetime(
            df["charttime"], utc=True
        )
        observation_df[FHIRObservation.value_quantity] = df.apply(
            lambda _df: Quantity(value=float(_df["urineoutput"]), unit="ml"),
            axis=1,
        )
        observation_df[FHIRObservation.identifier] = [
            Identifier(value=self._snomed_id, system="snomed")
        ] * len(df)

        return observation_df.pipe(DataFrame[FHIRObservation])

    def _to_ohdsi(self, df: DataFrame) -> DataFrame[AbstractOHDSISinkSchema]:
        raise NotImplementedError


class AbstractBgMapper(
    AbstractDatabaseSourceMapper[FHIRObservation, AbstractOHDSISinkSchema],
    metaclass=ABCMeta,
):
    SQL_QUERY = "SELECT * FROM mimiciv_derived.bg WHERE specimen = 'ART.';"  # and %(field)s IS NOT NULL;"
    VALUE_FIELD: str
    SQL_PARAMS = {}
    UNIT: str = ""

    def _to_fihr(self, df: DataFrame) -> DataFrame[FHIRObservation]:
        observation_df = pd.DataFrame()

        observation_df[FHIRObservation.subject] = df["subject_id"].map(
            lambda id: Reference(reference=str(id), type="Patient")
        )
        observation_df[FHIRObservation.effective_date_time] = pd.to_datetime(
            df["charttime"], utc=True
        )
        observation_df[FHIRObservation.value_quantity] = df.apply(
            lambda _df: Quantity(value=float(_df[self.VALUE_FIELD]), unit=self.UNIT),
            axis=1,
        )
        observation_df[FHIRObservation.identifier] = [
            Identifier(value=self._snomed_id, system="snomed")
        ] * len(df)

        return observation_df.pipe(DataFrame[FHIRObservation])

    def _to_ohdsi(self, df: DataFrame) -> DataFrame[AbstractOHDSISinkSchema]:
        raise NotImplementedError


class ArterialPO2Mapper(AbstractBgMapper):
    VALUE_FIELD = "po2"


class ArterialPCO2Mapper(AbstractBgMapper):
    VALUE_FIELD = "pco2"


class ArterialPHMapper(AbstractBgMapper):
    VALUE_FIELD = "ph"


class ArterialBicarbonateMapper(AbstractBgMapper):
    VALUE_FIELD = "bicarbonate"


class ArterialBaseexcessMapper(AbstractBgMapper):
    SQL_QUERY = "SELECT * FROM mimiciv_derived.bg WHERE specimen = 'ART.' and baseexcess IS NOT NULL;"
    VALUE_FIELD = "baseexcess"


class FiO2Mapper(AbstractBgMapper):
    SQL_QUERY = "SELECT * FROM mimiciv_derived.bg WHERE fio2 IS NOT NULL;"
    VALUE_FIELD = "fio2"
