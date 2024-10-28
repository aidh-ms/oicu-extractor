from typing import Any

import pandas as pd
from pandera.typing import DataFrame

from icu_pipeline.schema.fhir import CodeableConcept, CodeableReference, Coding, Dosage, Period, Quantity, Reference
from icu_pipeline.schema.fhir.medication import FHIRMedicationStatement
from icu_pipeline.source import DataSource
from icu_pipeline.source.database import AbstractDatabaseSourceMapper
from icu_pipeline.source.utils import offset_to_timestamp, to_timestamp


class EICUInfusionDosageMapper(AbstractDatabaseSourceMapper[FHIRMedicationStatement]):
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
        **kwargs: dict[str, Any],
    ) -> None:
        super().__init__(fhir_schema=FHIRMedicationStatement, datasource=DataSource.MIMICIV, **kwargs)  # type: ignore[arg-type]
        self._source = "eicu"
        assert self._unit is not None, f"No Unit definition for MimicMedicationMapper '{schema+'.'+table}' given."

        self._id_field = "subject_id"
        # Create and map fields to normalized names
        fields = kwargs.pop("fields", {})

        if "rate" not in fields:
            fields["rate"] = "drugrate"
            constraints["drugrate"] = "not null"
        else:
            value = fields.get("value")
            assert isinstance(value, str)
            constraints[value] = "not null"
        if "patient_id" not in fields:
            fields["patient_id"] = "patienthealthsystemstayid"
        if "time" not in fields:
            fields["time"] = "hospitaladmittime24"
        if "year" not in fields:
            fields["year"] = "hospitaldischargeyear"
        if "time" not in fields:
            fields["time"] = "hospitaladmittime24"
        if "offset" not in fields:
            fields["offset"] = "infusionoffset"

        self._query_args = {
            "schema": schema,
            "table": table,
            "constraints": constraints,
            "fields": fields,
            "joins": {
                "eicu_crd.patient": {f"{schema}.{table}.patientunitstayid": "eicu_crd.patient.patientunitstayid"}
            },
            "order_by": ["infusionoffset"],
        }

    def _clean_df(self, df: DataFrame) -> DataFrame:
        cleaned_df = pd.DataFrame()
        for group, rows in df.groupby("patient_id"):
            patient_df = rows.copy()

            # remove all values before icu admission
            patient_df.loc[patient_df["offset"] < 0, "offset"] = 0
            patient_df = patient_df.drop_duplicates(keep="last", subset=["offset"])

            # calculate start and end time of each infusion
            patient_df["start"] = patient_df.apply(
                lambda _df: offset_to_timestamp(
                    to_timestamp(_df["time"], _df["year"]),
                    _df["offset"],
                ),
                axis=1,
            )
            patient_df["end"] = patient_df["start"].shift(-1)
            patient_df["delta"] = (patient_df["end"] - patient_df["start"]).dt.total_seconds() // 60
            patient_df = patient_df.dropna(subset=["delta"])

            # remoce all rows with negative or zero rates
            patient_df["rate"] = patient_df["rate"].astype(float)
            patient_df = patient_df[patient_df["rate"] > 0]

            patient_df["value"] = patient_df["rate"] * patient_df["delta"]
            cleaned_df = pd.concat([cleaned_df, patient_df])

        return cleaned_df.reset_index(drop=True).pipe(DataFrame)

    def _to_fihr(self, df: DataFrame) -> DataFrame[FHIRMedicationStatement]:
        df = self._clean_df(df.pipe(DataFrame))

        medication_df = pd.DataFrame()
        medication_df[FHIRMedicationStatement.subject] = df["patient_id"].map(
            lambda id: Reference(reference=str(id), type=f"{self._data_source}")
        )

        medication_df[FHIRMedicationStatement.medication] = [
            CodeableReference(concept=CodeableConcept(coding=Coding(code=self._concept_id, system=self._concept_type)))
        ] * len(df)

        medication_df[FHIRMedicationStatement.dosage] = df.apply(
            lambda _df: Dosage(
                dose_quantity=Quantity(value=float(_df["value"]), unit=self._unit),
                rate_quantity=Quantity(value=float(_df["rate"]), unit=f"{self._unit}/min"),
            ),
            axis=1,
        )

        medication_df[FHIRMedicationStatement.effective_period] = df.apply(
            lambda _df: Period(start=_df["start"], end=_df["end"]), axis=1
        )

        return medication_df.pipe(DataFrame[FHIRMedicationStatement])
