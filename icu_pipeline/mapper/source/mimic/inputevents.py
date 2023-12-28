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
        medication_df = pd.DataFrame()

        medication_df[FHIRMedicationStatement.subject] = df["subject_id"].map(
            lambda id: Reference(reference=str(id), type="Patient")
        )
        medication_df[FHIRMedicationStatement.effective_period] = df.apply(
            lambda _df: Period(
                start=pd.to_datetime(_df["starttime"], utc=True),
                end=pd.to_datetime(_df["endtime"], utc=True),
            ),
            axis=1,
        )
        medication_df[FHIRMedicationStatement.medication] = [
            CodeableReference(
                concept=CodeableConcept(coding=Coding(code=self._id, system="snomed"))
            )
        ] * len(df)
        medication_df[FHIRMedicationStatement.dosage] = df.apply(
            lambda _df: Dosage(
                dose_quantity=Quantity(value=_df["amount"], unit=_df["amountuom"]),
                rate_quantity=Quantity(value=_df["rate"], unit=_df["rateuom"]),
            ),
            axis=1,
        )

        return medication_df.pipe(DataFrame[FHIRMedicationStatement])

    def _to_ohdsi(self, df: DataFrame) -> DataFrame[AbstractOHDSISinkSchema]:
        raise NotImplementedError


class NorepinephrineMapper(AbstractMimicInputEventMapper):
    SQL_PARAMS = {"values": [221906]}


class AdrenalineMapper(AbstractMimicInputEventMapper):
    SQL_PARAMS = {"values": [221289, 229617]}


class VasopressineMapper(AbstractMimicInputEventMapper):
    SQL_PARAMS = {"values": [222315]}


class DobutamineMapper(AbstractMimicInputEventMapper):
    SQL_PARAMS = {"values": [221653]}


class VancomycineMapper(AbstractMimicInputEventMapper):
    SQL_PARAMS = {"values": [225798]}
