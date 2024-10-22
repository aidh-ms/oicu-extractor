from typing import Any

import pandas as pd
from pandera.typing import DataFrame

from icu_pipeline.source import DataSource
from icu_pipeline.source.database import AbstractDatabaseSourceMapper
from icu_pipeline.schema.fhir import CodeableReference, CodeableConcept, Coding, Reference, Quantity, Dosage, Period
from icu_pipeline.schema.fhir.medication import FHIRMedicationStatement


class MimicDosageMapper(AbstractDatabaseSourceMapper[FHIRMedicationStatement]):
    """
    Mapper class that maps the MIMIC-IV data to the FHIR Dosage schema.
      This maps non-continuous drug administrations to a simple MedicationAdministration object.
      'Rate' is not available for such items.
    """

    def __init__(
        self,
        schema: str,
        table: str,
        constraints: dict[str, Any],
        unit: str,
        **kwargs: dict[str, Any],
    ) -> None:
        super().__init__(fhir_schema=FHIRMedicationStatement, datasource=DataSource.MIMICIV, **kwargs)
        self._source = "mimiciv"
        self._unit = unit
        assert self._unit is not None, f"No Unit definition for MimicMedicationMapper '{schema+'.'+table}' given."

        self._id_field = "subject_id"
        # Create and map fields to normalized names
        fields = kwargs.pop("fields", {})
        if "patient_id" not in fields:
            fields["patient_id"] = "subject_id"
        if "timestamp" not in fields:
            fields["timestamp"] = "starttime"
        if "value" not in fields:
            fields["value"] = "amount"
        self._query_args = {"schema": schema, "table": table, "constraints": constraints, "fields": fields}

    def _to_fihr(self, df: DataFrame) -> DataFrame[FHIRMedicationStatement]:
        medication_df = pd.DataFrame()

        medication_df[FHIRMedicationStatement.subject] = df["patient_id"].map(
            lambda id: Reference(reference=str(id), type=f"{self._data_source}")
        )

        medication_df[FHIRMedicationStatement.medication] = [
            CodeableReference(concept=CodeableConcept(coding=Coding(code=self._concept_id, system=self._concept_type)))
        ] * len(df)

        medication_df[FHIRMedicationStatement.dosage] = df.apply(
            lambda _df: Dosage(
                dose_quantity=Quantity(value=_df["value"], unit=self._unit),
                rate_quantity=Quantity(value=1.0, unit="unit"),
            ),
            axis=1,
        )

        medication_df[FHIRMedicationStatement.effective_period] = df.apply(
            lambda _df: Period(start=_df["timestamp"], end=_df["timestamp"]), axis=1
        )

        return medication_df.pipe(DataFrame[FHIRMedicationStatement])
