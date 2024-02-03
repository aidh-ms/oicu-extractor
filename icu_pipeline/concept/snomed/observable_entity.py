from icu_pipeline.concept.snomed import AbstractSnomedConcept
from icu_pipeline.mapper.schema.fhir.observation import FHIRObservation
from icu_pipeline.mapper.schema.ohdsi import AbstractOHDSISinkSchema
from icu_pipeline.mapper.source import DataSource
from icu_pipeline.mapper.source.amds.numericitems import (
    AmdsHeartRateMapper,
    AmdsSystolicBloodPressureInvasiveMapper,
    AmdsDiastolicBloodPressureInvasiveMapper,
    AmdsMeanArterialBloodPressureInvasiveMapper,
)
from icu_pipeline.mapper.source.mimic.chartevent import (
    MimicHeartRateMapper,
    MimicSystolicBloodPressureInvasiveMapper,
    MimicDiastolicBloodPressureInvasiveMapper,
    MimicMeanArterialBloodPressureInvasiveMapper,
    MimicSystolicBloodPressureNonInvasiveMapper,
    MimicDiastolicBloodPressureNonInvasiveMapper,
    MimicMeanArterialBloodPressureNonInvasiveMapper,
    MimicTemperatureMapper,
    MimicTemperatureFahrenheitMapper,
    MimicOxygenSaturationMapper,
)
from icu_pipeline.mapper.source.mimic.derived import (
    MimicUrineOutputMapper,
    MimicFiO2Mapper,
)
from icu_pipeline.mapper.source.mimic.labevents import (
    MimicArterialBloodLactateMapper,
    MimicLeukocyteCountMapper,
    MimicPlateletCountMapper,
    MimicCRPMapper,
    MimicGlucoseMapper,
    MimicMagnesiumMapper,
    MimicCalciumMapper,
    MimicPhosphateMapper,
)
from icu_pipeline.mapper.source.mimic.patient import (
    MimicAgeMapper,
    MimicGenderMapper,
)
from icu_pipeline.mapper.source.mimic.chartevent import (
    MimicHeightMapper,
)
from icu_pipeline.mapper.source.eicu.vitalperiodic import (
    EICUHeartRateMapper,
)
from icu_pipeline.mapper.source.eicu.vitalaperiodic import (
    EICUSystolicBloodPressureNonInvasiveMapper,
    EICUDiastolicBloodPressureNonInvasiveMapper,
    EICUMeanArterialBloodPressureNonInvasiveMapper,
)


class HeartRate(AbstractSnomedConcept):
    CONCEPT_ID = "364075005"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {
        DataSource.MIMIC: MimicHeartRateMapper,
        DataSource.AMDS: AmdsHeartRateMapper,
        DataSource.EICU: EICUHeartRateMapper,
    }


class SystolicBloodPressureInvasive(AbstractSnomedConcept):
    CONCEPT_ID = "251071003"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {
        DataSource.MIMIC: MimicSystolicBloodPressureInvasiveMapper,
        DataSource.AMDS: AmdsSystolicBloodPressureInvasiveMapper,
    }


class DiastolicBloodPressureInvasive(AbstractSnomedConcept):
    CONCEPT_ID = "251073000"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {
        DataSource.MIMIC: MimicDiastolicBloodPressureInvasiveMapper,
        DataSource.AMDS: AmdsDiastolicBloodPressureInvasiveMapper,
    }


class MeanArterialBloodPressureInvasive(AbstractSnomedConcept):
    CONCEPT_ID = "251074006"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {
        DataSource.MIMIC: MimicMeanArterialBloodPressureInvasiveMapper,
        DataSource.AMDS: AmdsMeanArterialBloodPressureInvasiveMapper,
    }


class SystolicBloodPressureNonInvasive(AbstractSnomedConcept):
    CONCEPT_ID = "251070002"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {
        DataSource.MIMIC: MimicSystolicBloodPressureNonInvasiveMapper,
        DataSource.EICU: EICUSystolicBloodPressureNonInvasiveMapper,
    }


class DiastolicBloodPressureNonInvasive(AbstractSnomedConcept):
    CONCEPT_ID = "174255007"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {
        DataSource.MIMIC: MimicDiastolicBloodPressureNonInvasiveMapper,
        DataSource.EICU: EICUDiastolicBloodPressureNonInvasiveMapper,
    }


class MeanArterialBloodPressureNonInvasive(AbstractSnomedConcept):
    CONCEPT_ID = "251074006"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {
        DataSource.MIMIC: MimicMeanArterialBloodPressureNonInvasiveMapper,
        DataSource.EICU: EICUMeanArterialBloodPressureNonInvasiveMapper,
    }


class UrineOutput(AbstractSnomedConcept):
    CONCEPT_ID = "404231008"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicUrineOutputMapper}


class Temperature(AbstractSnomedConcept):
    CONCEPT_ID = "386725007"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {
        DataSource.MIMIC: [MimicTemperatureFahrenheitMapper, MimicTemperatureMapper]
    }


class FiO2(AbstractSnomedConcept):
    CONCEPT_ID = "250774007"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicFiO2Mapper}


class OxygenSaturation(AbstractSnomedConcept):
    CONCEPT_ID = "431314004"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicOxygenSaturationMapper}


class ArterialBloodLactate(AbstractSnomedConcept):
    CONCEPT_ID = "372451000119107"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicArterialBloodLactateMapper}


class Height(AbstractSnomedConcept):
    CONCEPT_ID = "1153637007"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicHeightMapper}


class Age(AbstractSnomedConcept):
    CONCEPT_ID = "424144002"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicAgeMapper}


class Gender(AbstractSnomedConcept):
    CONCEPT_ID = "263495000"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicGenderMapper}


class LeukocyteCount(AbstractSnomedConcept):
    CONCEPT_ID = "4298431"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicLeukocyteCountMapper}


class PlateletCount(AbstractSnomedConcept):
    CONCEPT_ID = "4267147"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicPlateletCountMapper}


class CRP(AbstractSnomedConcept):
    CONCEPT_ID = "37398482"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicCRPMapper}


class Glucose(AbstractSnomedConcept):
    CONCEPT_ID = "37399654"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicGlucoseMapper}


class Magnesium(AbstractSnomedConcept):
    CONCEPT_ID = "4243005"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicMagnesiumMapper}


class Calcium(AbstractSnomedConcept):
    CONCEPT_ID = "4193434"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicCalciumMapper}


class Phosphate(AbstractSnomedConcept):
    CONCEPT_ID = "4194292"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicPhosphateMapper}
