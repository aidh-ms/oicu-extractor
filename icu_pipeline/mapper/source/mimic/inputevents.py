from abc import ABCMeta

import pandas as pd
from pandera.typing import DataFrame

from icu_pipeline.mapper.source import AbstractDatabaseSourceMapper
from icu_pipeline.mapper.schema.ohdsi import AbstractOHDSISinkSchema
from icu_pipeline.mapper.schema.fhir import (
    CodeableReference,
    CodeableConcept,
    Coding,
    Reference,
    Quantity,
    Period,
)
from icu_pipeline.mapper.schema.fhir.medication import FHIRMedicationStatement, Dosage


class AbstractMimicInputEventMapper(
    AbstractDatabaseSourceMapper[FHIRMedicationStatement, AbstractOHDSISinkSchema],
    metaclass=ABCMeta,
):
    SQL_QUERY = "SELECT * FROM mimiciv_icu.inputevents WHERE itemid = any(%(values)s);"

    def _to_fihr(self, df: DataFrame) -> DataFrame[FHIRMedicationStatement]:
        observation_df = pd.DataFrame()

        observation_df[FHIRMedicationStatement.subject] = df["subject_id"].map(
            lambda id: Reference(reference=str(id), type="Patient")
        )
        observation_df[FHIRMedicationStatement.effective_period] = df.apply(
            lambda _df: Period(
                start=pd.to_datetime(_df["starttime"], utc=True),
                end=pd.to_datetime(_df["endtime"], utc=True),
            ),
            axis=1,
        )
        observation_df[FHIRMedicationStatement.medication] = [
            CodeableReference(
                concept=CodeableConcept(
                    coding=Coding(code=self._snomed_id, system="snomed")
                )
            )
        ] * len(df)
        observation_df[FHIRMedicationStatement.dosage] = df.apply(
            lambda _df: Dosage(
                dose_quantity=Quantity(value=_df["amount"], unit=_df["amountuom"]),
                rate_quantity=Quantity(value=_df["rate"], unit=_df["rateuom"]),
            ),
            axis=1,
        )

        return observation_df.pipe(DataFrame[FHIRMedicationStatement])

    def _to_ohdsi(self, df: DataFrame) -> DataFrame[AbstractOHDSISinkSchema]:
        raise NotImplementedError


class NorepinephrineMapper(AbstractMimicInputEventMapper):
    SQL_PARAMS = {"values": [221906]}
