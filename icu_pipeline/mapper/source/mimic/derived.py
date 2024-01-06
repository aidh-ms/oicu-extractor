import pandas as pd
from pandera.typing import DataFrame

from icu_pipeline.mapper.source import AbstractDatabaseSourceMapper
from icu_pipeline.mapper.source.mimic import AbstractMimicEventsMapper
from icu_pipeline.mapper.schema.ohdsi import AbstractOHDSISinkSchema
from icu_pipeline.mapper.schema.fhir import (
    CodeableReference,
    CodeableConcept,
    Coding,
    Reference,
)
from icu_pipeline.mapper.schema.fhir.deviceusage import FHIRDeviceUsage


class UrineOutputMapper(AbstractMimicEventsMapper):
    SQL_QUERY = """
    SELECT 
        subject_id, charttime, 
        SUM(CASE
            WHEN oe.itemid = 227488 AND oe.value > 0 THEN -1 * oe.value
            ELSE oe.value
        END) AS valuenum,
        'ml' as valueuom
    FROM mimiciv_icu.outputevents oe
    WHERE itemid = any(%(values)s)
    GROUP BY subject_id, charttime, valueuom;"""
    SQL_PARAMS = {"values": [226559, 226560, 226561, 226584, 226563, 226564, 226565, 226567, 226557, 226558, 227488, 227489]}  # fmt: skip


class ArterialPO2Mapper(AbstractMimicEventsMapper):
    SQL_QUERY = "SELECT *, po2 as valuenum, 'mmHg' as valueuom FROM mimiciv_derived.bg WHERE specimen = 'ART.';"


class ArterialPCO2Mapper(AbstractMimicEventsMapper):
    SQL_QUERY = "SELECT *, pco2 as valuenum, 'mmHg' as valueuom FROM mimiciv_derived.bg WHERE specimen = 'ART.';"


class ArterialPHMapper(AbstractMimicEventsMapper):
    SQL_QUERY = "SELECT *, ph as valuenum, 'mmHg' as valueuom FROM mimiciv_derived.bg WHERE specimen = 'ART.';"


class ArterialBicarbonateMapper(AbstractMimicEventsMapper):
    SQL_QUERY = "SELECT *, bicarbonate as valuenum, 'mmHg' as valueuom FROM mimiciv_derived.bg WHERE specimen = 'ART.';"


class ArterialBaseexcessMapper(AbstractMimicEventsMapper):
    SQL_QUERY = "SELECT *, baseexcess as valuenum, 'mmHg' as valueuom FROM mimiciv_derived.bg WHERE specimen = 'ART.'  and baseexcess IS NOT NULL;;"


class FiO2Mapper(AbstractMimicEventsMapper):
    SQL_QUERY = "SELECT *, fio2 as valuenum, 'mmHg' as valueuom FROM mimiciv_derived.bg;"  # fmt: skip


class DialysisMapper(
    AbstractDatabaseSourceMapper[FHIRDeviceUsage, AbstractOHDSISinkSchema],
):
    SQL_QUERY = """SELECT mimiciv_derived.rrt.*, subject_id FROM mimiciv_derived.rrt JOIN mimiciv_icu.icustays ON mimiciv_derived.rrt.stay_id=mimiciv_icu.icustays.stay_id"""
    SQL_PARAMS = {}

    def _to_fihr(self, df: DataFrame) -> DataFrame[FHIRDeviceUsage]:
        device_usage_df = pd.DataFrame()

        device_usage_df[FHIRDeviceUsage.patient] = df["subject_id"].map(
            lambda id: Reference(reference=str(id), type="MIMIC-Patient")
        )
        device_usage_df[FHIRDeviceUsage.timing_date_time] = pd.to_datetime(
            df["charttime"], utc=True
        )
        device_usage_df[FHIRDeviceUsage.device] = [
            CodeableReference(
                concept=CodeableConcept(
                    coding=Coding(code=self._concept_id, system=self._concept_type)
                )
            )
        ] * len(df)

        return device_usage_df.pipe(DataFrame[FHIRDeviceUsage])

    def _to_ohdsi(self, df: DataFrame) -> DataFrame[AbstractOHDSISinkSchema]:
        raise NotImplementedError
