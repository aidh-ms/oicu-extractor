from icu_pipeline.concept import AbstractSnomedConcept
from icu_pipeline.mapper.schema.fhir.observation import FHIRObservation
from icu_pipeline.mapper.schema.ohdsi import AbstractOHDSISinkSchema
from icu_pipeline.mapper.source import DataSource
from icu_pipeline.mapper.source.mimic.labevents import MimicSerumCreatinineMapper
from icu_pipeline.mapper.source.mimic.chartevent import (
    MimicHeartRateMapper,
    SystolicBloodPressureInvasiveMapper,
    DiastolicBloodPressureInvasiveMapper,
    MeanArterialBloodPressureInvasiveMapper,
    SystolicBloodPressureNonInvasiveMapper,
    DiastolicBloodPressureNonInvasiveMapper,
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
