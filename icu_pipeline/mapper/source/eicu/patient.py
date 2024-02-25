import pandas as pd

from abc import ABCMeta

from pandera.typing import DataFrame

from icu_pipeline.mapper.source import AbstractDatabaseSourceMapper
from icu_pipeline.mapper.source.utils import offset_to_period
from icu_pipeline.mapper.schema.ohdsi import AbstractOHDSISinkSchema
from icu_pipeline.mapper.schema.fhir import (
    Reference,
    Period,
)
from icu_pipeline.mapper.schema.fhir.encounter import FHIREncounter
from icu_pipeline.mapper.source.eicu import AbstractEICUVitalMapper


class EICUEncounterMapper(
    AbstractDatabaseSourceMapper[FHIREncounter, AbstractOHDSISinkSchema],
):
    SQL_QUERY = "SELECT * FROM eicu_crd.patient;"

    def _to_fihr(self, df: DataFrame) -> DataFrame[FHIREncounter]:
        encounter_df = pd.DataFrame()

        encounter_df[FHIREncounter.subject] = df["patienthealthsystemstayid"].map(
            lambda id: Reference(reference=str(id), type="eICU-Patient")
        )
        encounter_df[FHIREncounter.actual_period] = df.apply(
            lambda _df: offset_to_period(
                _df["hospitaldischargeyear"],
                _df["hospitaladmittime24"],
                _df["hospitaladmitoffset"] * -1,
                _df["unitdischargeoffset"],
            ),
            axis=1,
        )
        encounter_df[FHIREncounter.care_team] = df["unittype"].map(
            lambda id: Reference(reference=str(id), type="CareTeam")
        )

        return encounter_df.pipe(DataFrame[FHIREncounter])

    def _to_ohdsi(self, df: DataFrame) -> DataFrame[AbstractOHDSISinkSchema]:
        raise NotImplementedError


class AbstractEICUDemographicMapper(AbstractEICUVitalMapper, metaclass=ABCMeta):
    SQL_QUERY = """
        SELECT DISTINCT ON(patienthealthsystemstayid) *, hospitaladmitoffset * -1 as observationoffset, {field} as value FROM eicu_crd.patient 
    """


class EICUAgeMapper(AbstractEICUDemographicMapper):
    SQL_FIELDS = {"field": "age"}
    UNIT = ""  # TODO


class EICUGenderMapper(AbstractEICUDemographicMapper):
    SQL_FIELDS = {"field": "gender"}
    UNIT = ""  # TODO


class EICUHeightMapper(AbstractEICUDemographicMapper):
    SQL_FIELDS = {"field": "admissionheight"}
    UNIT = ""  # TODO


class EICUWeightMapper(AbstractEICUDemographicMapper):
    SQL_FIELDS = {"field": "admissionweight"}
    UNIT = ""  # TODO
