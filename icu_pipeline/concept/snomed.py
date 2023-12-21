from icu_pipeline.concept import AbstractSnomedConcept
from icu_pipeline.mapper.schema.fhir.observation import FHIRObservation
from icu_pipeline.mapper.schema.ohdsi import AbstractOHDSISinkSchema
from icu_pipeline.mapper.source import DataSource
from icu_pipeline.mapper.source.mimic.labevents import (
    MimicSerumCreatinineMapper,
    UreaMapper,
    HbMapper,
    ArterialBloodLactateMapper,
    BloodSodiumMapper,
)
from icu_pipeline.mapper.source.mimic.chartevent import (
    MimicHeartRateMapper,
    SystolicBloodPressureInvasiveMapper,
    DiastolicBloodPressureInvasiveMapper,
    MeanArterialBloodPressureInvasiveMapper,
    SystolicBloodPressureNonInvasiveMapper,
    DiastolicBloodPressureNonInvasiveMapper,
    MeanArterialBloodPressureNonInvasiveMapper,
    OxygenSaturationMapper,
)


class SerumCreatinine(AbstractSnomedConcept):
    SNOMED_ID = "113075003"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicSerumCreatinineMapper}


class HeartRate(AbstractSnomedConcept):
    SNOMED_ID = "364075005"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicHeartRateMapper}


class SystolicBloodPressureInvasive(AbstractSnomedConcept):
    SNOMED_ID = "251071003"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: SystolicBloodPressureInvasiveMapper}


class DiastolicBloodPressureInvasive(AbstractSnomedConcept):
    SNOMED_ID = "251073000"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: DiastolicBloodPressureInvasiveMapper}


class MeanArterialBloodPressureInvasive(AbstractSnomedConcept):
    SNOMED_ID = "251074006"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MeanArterialBloodPressureInvasiveMapper}


class SystolicBloodPressureNonInvasive(AbstractSnomedConcept):
    SNOMED_ID = "251070002"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: SystolicBloodPressureNonInvasiveMapper}


class DiastolicBloodPressureNonInvasive(AbstractSnomedConcept):
    SNOMED_ID = "174255007"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: DiastolicBloodPressureNonInvasiveMapper}


class MeanArterialBloodPressureNonInvasive(AbstractSnomedConcept):
    SNOMED_ID = "251074006"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MeanArterialBloodPressureNonInvasiveMapper}


class OxygenSaturation(AbstractSnomedConcept):
    SNOMED_ID = "431314004"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: OxygenSaturationMapper}


class Urea(AbstractSnomedConcept):
    SNOMED_ID = "250623007"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: UreaMapper}


class Hb(AbstractSnomedConcept):
    SNOMED_ID = "104141003"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: HbMapper}


class ArterialBloodLactate(AbstractSnomedConcept):
    SNOMED_ID = "372451000119107"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: ArterialBloodLactateMapper}


class BloodSodium(AbstractSnomedConcept):
    SNOMED_ID = "312469006"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: BloodSodiumMapper}
