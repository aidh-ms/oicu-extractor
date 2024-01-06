from icu_pipeline.concept.snomed import AbstractSnomedConcept
from icu_pipeline.mapper.schema.fhir.observation import FHIRObservation
from icu_pipeline.mapper.schema.ohdsi import AbstractOHDSISinkSchema
from icu_pipeline.mapper.source import DataSource
from icu_pipeline.mapper.source.mimic.chartevent import (
    MimicHeartRateMapper,
    SystolicBloodPressureInvasiveMapper,
    DiastolicBloodPressureInvasiveMapper,
    MeanArterialBloodPressureInvasiveMapper,
    SystolicBloodPressureNonInvasiveMapper,
    DiastolicBloodPressureNonInvasiveMapper,
    MeanArterialBloodPressureNonInvasiveMapper,
    TemperatureMapper,
    TemperatureFahrenheitMapper,
    OxygenSaturationMapper,
)
from icu_pipeline.mapper.source.mimic.derived import (
    UrineOutputMapper,
    FiO2Mapper,
)
from icu_pipeline.mapper.source.mimic.labevents import (
    ArterialBloodLactateMapper,
)
from icu_pipeline.mapper.source.mimic.patient import (
    AgeMapper,
    GenderMapper,
)
from icu_pipeline.mapper.source.mimic.chartevent import (
    HeightMapper,
)


class HeartRate(AbstractSnomedConcept):
    CONCEPT_ID = "364075005"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicHeartRateMapper}


class SystolicBloodPressureInvasive(AbstractSnomedConcept):
    CONCEPT_ID = "251071003"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: SystolicBloodPressureInvasiveMapper}


class DiastolicBloodPressureInvasive(AbstractSnomedConcept):
    CONCEPT_ID = "251073000"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: DiastolicBloodPressureInvasiveMapper}


class MeanArterialBloodPressureInvasive(AbstractSnomedConcept):
    CONCEPT_ID = "251074006"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MeanArterialBloodPressureInvasiveMapper}


class SystolicBloodPressureNonInvasive(AbstractSnomedConcept):
    CONCEPT_ID = "251070002"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: SystolicBloodPressureNonInvasiveMapper}


class DiastolicBloodPressureNonInvasive(AbstractSnomedConcept):
    CONCEPT_ID = "174255007"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: DiastolicBloodPressureNonInvasiveMapper}


class MeanArterialBloodPressureNonInvasive(AbstractSnomedConcept):
    CONCEPT_ID = "251074006"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MeanArterialBloodPressureNonInvasiveMapper}


class UrineOutput(AbstractSnomedConcept):
    CONCEPT_ID = "404231008"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: UrineOutputMapper}


class Temperature(AbstractSnomedConcept):
    CONCEPT_ID = "386725007"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: [TemperatureFahrenheitMapper, TemperatureMapper]}


class FiO2(AbstractSnomedConcept):
    CONCEPT_ID = "250774007"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: FiO2Mapper}


class OxygenSaturation(AbstractSnomedConcept):
    CONCEPT_ID = "431314004"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: OxygenSaturationMapper}


class ArterialBloodLactate(AbstractSnomedConcept):
    CONCEPT_ID = "372451000119107"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: ArterialBloodLactateMapper}


class Height(AbstractSnomedConcept):
    CONCEPT_ID = "1153637007"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: HeightMapper}


class Age(AbstractSnomedConcept):
    CONCEPT_ID = "424144002"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: AgeMapper}


class Gender(AbstractSnomedConcept):
    CONCEPT_ID = "263495000"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: GenderMapper}
